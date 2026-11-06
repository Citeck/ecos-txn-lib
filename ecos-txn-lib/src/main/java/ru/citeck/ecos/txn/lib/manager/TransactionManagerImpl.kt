package ru.citeck.ecos.txn.lib.manager

import io.github.oshai.kotlinlogging.KotlinLogging
import org.slf4j.MDC
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.micrometer.EcosMicrometerContext
import ru.citeck.ecos.txn.lib.action.TxnActionId
import ru.citeck.ecos.txn.lib.action.TxnActionRef
import ru.citeck.ecos.txn.lib.action.TxnActionType
import ru.citeck.ecos.txn.lib.commit.CommitCoordinator
import ru.citeck.ecos.txn.lib.commit.CommitCoordinatorImpl
import ru.citeck.ecos.txn.lib.commit.TxnCommitData
import ru.citeck.ecos.txn.lib.commit.repo.AuthoritativeTwoPhaseCommitRepo
import ru.citeck.ecos.txn.lib.commit.repo.JournalPresence
import ru.citeck.ecos.txn.lib.commit.repo.NoopTwoPhaseCommitRepo
import ru.citeck.ecos.txn.lib.commit.repo.TwoPhaseCommitRepo
import ru.citeck.ecos.txn.lib.manager.action.TxnActionsContainer
import ru.citeck.ecos.txn.lib.manager.action.TxnActionsManager
import ru.citeck.ecos.txn.lib.manager.api.client.ApiVersionRes
import ru.citeck.ecos.txn.lib.manager.api.client.CurrentAppClientWrapper
import ru.citeck.ecos.txn.lib.manager.api.client.TxnManagerRemoteApiClient
import ru.citeck.ecos.txn.lib.manager.api.client.TxnManagerWebApiClient
import ru.citeck.ecos.txn.lib.manager.obs.TxnActionObsContext
import ru.citeck.ecos.txn.lib.manager.recovery.RecoveryManager
import ru.citeck.ecos.txn.lib.manager.recovery.RecoveryManagerImpl
import ru.citeck.ecos.txn.lib.manager.recovery.StartupOrphanRecovery
import ru.citeck.ecos.txn.lib.manager.work.AlwaysStatelessExtTxnWorkContext
import ru.citeck.ecos.txn.lib.manager.work.ExtTxnWorkContext
import ru.citeck.ecos.txn.lib.transaction.*
import ru.citeck.ecos.txn.lib.transaction.ctx.TxnManagerContext
import ru.citeck.ecos.txn.lib.transaction.xid.EcosXid
import ru.citeck.ecos.webapp.api.EcosWebAppApi
import ru.citeck.ecos.webapp.api.properties.EcosWebAppProps
import ru.citeck.ecos.webapp.api.task.executor.EcosTaskExecutorApi
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import kotlin.collections.LinkedHashMap
import kotlin.collections.LinkedHashSet

class TransactionManagerImpl : TransactionManager {

    companion object {
        private val log = KotlinLogging.logger {}

        private const val MDC_TXN_ID_KEY = "ecosTxnId"
    }

    internal lateinit var props: EcosTxnProps
    internal lateinit var webAppApi: EcosWebAppApi
    internal lateinit var micrometerContext: EcosMicrometerContext
    internal lateinit var actionsManager: TxnActionsManager
    internal lateinit var commitCoordinator: CommitCoordinator
    internal lateinit var remoteClient: TxnManagerRemoteApiClient

    private lateinit var webAppProps: EcosWebAppProps
    private lateinit var txnActionsExecutor: EcosTaskExecutorApi
    private lateinit var txnManagerJob: TxnManagerJob
    private lateinit var recoveryManager: RecoveryManagerImpl
    private lateinit var twoPhaseCommitRepo: TwoPhaseCommitRepo

    private val currentTxn = ThreadLocal<ManagedTransaction>()
    internal val transactionsById = ConcurrentHashMap<TxnId, TransactionInfo>()
    internal val abortedBranches = AbortedBranches()

    @JvmOverloads
    fun init(
        webAppApi: EcosWebAppApi,
        props: EcosTxnProps = EcosTxnProps(),
        micrometerContext: EcosMicrometerContext = EcosMicrometerContext.NOOP,
        twoPhaseCommitRepo: TwoPhaseCommitRepo = NoopTwoPhaseCommitRepo,
        remoteClient: TxnManagerRemoteApiClient? = null
    ) {
        this.props = props
        this.twoPhaseCommitRepo = twoPhaseCommitRepo
        this.webAppApi = webAppApi
        this.webAppProps = webAppApi.getProperties()
        this.txnActionsExecutor = webAppApi.getTasksApi().getExecutor("txn-actions")
        this.remoteClient = CurrentAppClientWrapper(
            remoteClient ?: TxnManagerWebApiClient(
                webAppApi.getWebClientApi(),
                webAppApi.getRemoteWebAppsApi()
            ),
            this
        )

        this.micrometerContext = micrometerContext
        this.actionsManager = TxnActionsManager(this)
        this.commitCoordinator = CommitCoordinatorImpl(twoPhaseCommitRepo, this)
        this.txnManagerJob = TxnManagerJob(this)
        this.recoveryManager = RecoveryManagerImpl()

        webAppApi.doWhenAppReady { this.txnManagerJob.start() }
    }

    override fun getJournalPresence(coordinatorApp: String, txnId: TxnId): JournalPresence {

        val appRefs = setOf(webAppProps.appName, webAppProps.appName + ":" + webAppProps.appInstanceId)
        if (props.commitCoordinatorApp.isBlank() ||
            coordinatorApp !in appRefs ||
            props.commitCoordinatorApp !in appRefs ||
            twoPhaseCommitRepo !is AuthoritativeTwoPhaseCommitRepo ||
            txnId.isEmpty()
        ) {
            return JournalPresence.UNKNOWN
        }
        if (transactionsById.containsKey(txnId) || twoPhaseCommitRepo.getRecoveryData(txnId) != null) {
            return JournalPresence.PRESENT
        }
        return JournalPresence.ABSENT
    }

    internal fun createStartupOrphanRecovery(): StartupOrphanRecovery {

        val coordinator = props.commitCoordinatorApp.ifBlank { webAppProps.appName }
        return StartupOrphanRecovery(
            recoveryManager,
            canAttempt = {
                props.commitCoordinatorApp.isNotBlank() &&
                    remoteClient.isAppAvailable(coordinator) &&
                    remoteClient.isApiVersionSupported(coordinator, TxnManagerRemoteApiClient.JOURNAL_PRESENCE_VER) ==
                    ApiVersionRes.SUPPORTED
            },
            isTransactionActive = { transactionsById.containsKey(it) }
        ) { txnId ->
            if (props.commitCoordinatorApp.isBlank()) {
                JournalPresence.UNKNOWN
            } else if (transactionsById.containsKey(txnId)) {
                JournalPresence.PRESENT
            } else if (!remoteClient.isAppAvailable(coordinator) ||
                remoteClient.isApiVersionSupported(coordinator, TxnManagerRemoteApiClient.JOURNAL_PRESENCE_VER) !=
                ApiVersionRes.SUPPORTED
            ) {
                JournalPresence.UNKNOWN
            } else {
                remoteClient.getJournalPresence(coordinator, txnId)
            }
        }
    }

    override fun coordinateCommit(txnId: TxnId, data: TxnCommitData, txnLevel: Int) {
        logDebug(txnId) { "Coordinate commit called. Data: $data txnLevel: $txnLevel" }
        commitCoordinator.commitRoot(txnId, data, txnLevel)
    }

    override fun <T> doInTxn(policy: TransactionPolicy, readOnly: Boolean?, action: () -> T): T {

        return when (policy) {
            TransactionPolicy.NOT_SUPPORTED -> doWithoutTxn(action)
            TransactionPolicy.REQUIRES_NEW -> doInNewTxn(readOnly ?: false, action)
            TransactionPolicy.SUPPORTS -> {
                val currentTxn = currentTxn.get()
                val newReadOnly = readOnly ?: currentTxn?.isReadOnly() ?: false
                if (currentTxn != null && currentTxn.isReadOnly() != newReadOnly) {
                    currentTxn.doWithinTxn(newReadOnly, action)
                } else {
                    action.invoke()
                }
            }

            TransactionPolicy.REQUIRED -> {
                val currentTxn = currentTxn.get()
                val newReadOnly = readOnly ?: currentTxn?.isReadOnly() ?: false
                if (currentTxn == null) {
                    doInNewTxn(newReadOnly, action)
                } else if (currentTxn.isReadOnly() != newReadOnly) {
                    currentTxn.doWithinTxn(newReadOnly, action)
                } else {
                    action.invoke()
                }
            }
        }
    }

    override fun <T> doInExtTxn(
        extTxnId: TxnId,
        extCtx: TxnManagerContext,
        policy: TransactionPolicy,
        readOnly: Boolean,
        nesting: TxnNesting,
        action: (ExtTxnWorkContext) -> T
    ): T {

        return when (policy) {
            TransactionPolicy.NOT_SUPPORTED -> doWithoutTxn { action(AlwaysStatelessExtTxnWorkContext) }
            TransactionPolicy.REQUIRES_NEW -> doInNewTxn(readOnly) { action(AlwaysStatelessExtTxnWorkContext) }
            TransactionPolicy.SUPPORTS -> {
                if (extTxnId.isEmpty()) {
                    action.invoke(AlwaysStatelessExtTxnWorkContext)
                } else {
                    doInExtTxn(extTxnId, readOnly, extCtx, nesting, action)
                }
            }

            TransactionPolicy.REQUIRED -> {
                if (extTxnId.isEmpty()) {
                    doInNewTxn(readOnly) { action.invoke(AlwaysStatelessExtTxnWorkContext) }
                } else {
                    doInExtTxn(extTxnId, readOnly, extCtx, nesting, action)
                }
            }
        }
    }

    private fun <T> doInExtTxn(
        extTxnId: TxnId,
        readOnly: Boolean,
        ctx: TxnManagerContext,
        incomingNesting: TxnNesting,
        action: (ExtTxnWorkContext) -> T
    ): T {

        // A read-only local txn that was kept alive for cache purposes can't host a
        // subsequent non-readonly action (TransactionImpl forbids RO→non-RO downgrade).
        // When the originator's next webapi call needs a writable txn, dispose the kept
        // RO local copy and create a fresh non-readonly one. The cached snapshot is
        // invalidated either way as soon as a writable action runs against the same data.
        val existing = transactionsById[extTxnId]
        val retainedNesting = existing?.transaction?.let { TxnNesting.of(it) }
        if (existing != null && existing.transaction.isReadOnly() && !readOnly) {
            dispose(extTxnId)
        }

        var isNewLocalTxn = false

        val transaction = transactionsById.computeIfAbsent(extTxnId) {
            val nesting = retainedNesting ?: incomingNesting
            require(nesting.parentId != extTxnId) { "Transaction cannot be its own parent" }
            isNewLocalTxn = true
            val txn = TransactionImpl(
                it,
                webAppProps.appName,
                readOnly,
                micrometerContext,
                nesting
            )
            txn.start()
            TransactionInfo(txn)
        }.transaction

        val workContext = object : ExtTxnWorkContext {
            override fun stopWork(): Boolean {
                // A read-only txn is kept alive only if it actually carries cached data:
                // tx-scoped caches stored via Transaction.getReadOnlyCache must survive across multiple
                // webapi calls sharing the same external txnId, so disposing on every reply
                // would reset the cache between calls. An empty read-only txn (no resources,
                // no actions, no data) is still disposed eagerly — same as the original behavior.
                val workWasStateless = isNewLocalTxn &&
                    transaction.isEmpty() &&
                    (!readOnly || !transaction.hasData())
                if (workWasStateless) {
                    try {
                        dispose(extTxnId)
                    } catch (e: Throwable) {
                        logError(extTxnId, e) { "Error while dispose local-external transaction" }
                    }
                } else if (isNewLocalTxn) {
                    // The local txn is being kept alive (readonly with cached data, or stateful
                    // work). Register the current app as a participant so the originating app
                    // notifies us when the root txn ends, after a failure too — without this the
                    // background TxnManagerJob would have to GC the lingering local txn after
                    // it goes idle. For stateful txns getOrAddRes already registered xids for
                    // the current app and this is a no-op on the receiving ctx (addAll of an
                    // empty set).
                    ctx.registerXids(webAppProps.appName, emptySet())
                }
                return !workWasStateless
            }
        }

        return try {
            doWithinTxn(transaction, readOnly, ctx) {
                action(workContext)
            }
        } finally {
            workContext.stopWork()
        }
    }

    private fun <T> doInNewTxn(readOnly: Boolean, action: () -> T): T {
        return doInNewTxn(readOnly, 0, action)
    }

    internal fun <T> doInNewTxn(readOnly: Boolean, txnLevel: Int, action: () -> T): T {

        val newTxnId = TxnId.create(webAppProps.appName, webAppProps.appInstanceId)

        log.debug {
            "Do in new txn called. " +
                "ReadOnly: $readOnly " +
                "level: $txnLevel " +
                "currentTxn: ${currentTxn.get()?.getId()} newTxn: $newTxnId"
        }

        val transactionStartTime = System.currentTimeMillis()

        if (txnLevel >= 10) {
            error("Transaction actions level overflow error")
        }
        val transaction = TransactionImpl(
            newTxnId,
            webAppProps.appName,
            readOnly,
            micrometerContext,
            TxnNesting.childOf(currentTxn.get())
        )

        val actions = TxnActionsContainer(newTxnId, this)
        val xidsByApp = LinkedHashMap<String, MutableSet<EcosXid>>()

        val txnManagerContext = object : TxnManagerContext {
            override fun registerAction(type: TxnActionType, actionRef: TxnActionRef) {
                if (!readOnly) {
                    registerXids(actionRef.appName, emptySet())
                    actions.addAction(type, actionRef)
                }
            }

            override fun registerXids(appName: String, xids: Collection<EcosXid>) {
                xidsByApp.computeIfAbsent(appName) { LinkedHashSet() }.addAll(xids)
            }
        }

        val result = doWithinTxn(transaction, readOnly, txnManagerContext) {

            var coordinationStarted = false
            try {
                transactionsById[newTxnId] = TransactionInfo(transaction)
                transaction.start()

                val txnActionObservation = micrometerContext.createObs(
                    TxnActionObsContext(newTxnId, this)
                )

                val actionRes = txnActionObservation.observe { action.invoke() }

                if (readOnly) {
                    releaseReadOnlyRoot(transaction, xidsByApp.keys)
                    actionRes
                } else {
                    AuthContext.runAsSystem {
                        actions.executeBeforeCommitActions()
                        val twopcActions = EnumMap<TxnActionType, List<TxnActionId>>(TxnActionType::class.java)
                        val afterCommitActions = actions.getActions(TxnActionType.AFTER_COMMIT)
                        if (afterCommitActions.isNotEmpty()) {
                            twopcActions[TxnActionType.AFTER_COMMIT] = afterCommitActions
                        }
                        val afterRollbackActions = actions.getActions(TxnActionType.AFTER_ROLLBACK)
                        if (afterRollbackActions.isNotEmpty()) {
                            twopcActions[TxnActionType.AFTER_ROLLBACK] = afterRollbackActions
                        }
                        val twopcCommitData = TxnCommitData(xidsByApp, twopcActions)
                        coordinationStarted = true
                        commitCoordinator.commitRoot(newTxnId, twopcCommitData, txnLevel)

                        actionRes
                    }
                }
            } catch (error: Throwable) {
                AuthContext.runAsSystem {
                    if (coordinationStarted) {
                        return@runAsSystem
                    }
                    if (readOnly) {
                        releaseReadOnlyRoot(transaction, xidsByApp.keys, error)
                    } else if (transaction.getStatus() == TransactionStatus.COMMITTED ||
                        transaction.getStatus() == TransactionStatus.ROLLED_BACK
                    ) {
                        // transaction may be disposed early by commit coordinator
                        if (transactionsById.containsKey(newTxnId)) {
                            dispose(newTxnId)
                        }
                    } else if (transaction.isEmpty() && xidsByApp.isEmpty()) {
                        // No resource or participant was enlisted and coordination has not started.
                        // Journal cleanup would start another transaction and recurse on pool failure.
                        dispose(newTxnId)
                    } else {
                        commitCoordinator.rollbackRoot(
                            newTxnId,
                            xidsByApp.keys,
                            actions.getActions(TxnActionType.AFTER_ROLLBACK),
                            error,
                            txnLevel
                        )
                    }
                }
                throw error
            }
        }

        val totalTime = System.currentTimeMillis() - transactionStartTime
        logDebug(transaction) { "Do in new txn finished. ReadOnly: $readOnly level: $txnLevel. Total time: $totalTime ms" }

        return result
    }

    /**
     * Completes a read-only root after its action returned or threw: the local branch is disposed
     * first, then every remote participant is told to release its branch. The participants hold
     * their branches until they are told, so both outcomes must release them. A [failure] that the
     * caller rethrows is never replaced: a failed local dispose is attached to it as suppressed.
     * Without a failure, a failed local dispose is thrown before the notification, and the catch of
     * doInNewTxn calls this again with it, so the participants are still notified once.
     */
    private fun releaseReadOnlyRoot(
        transaction: ManagedTransaction,
        participants: Collection<String>,
        failure: Throwable? = null
    ) {
        // local dispose stays synchronous: the one-phase commit of read-only resources happens inside
        // transaction.dispose(), and callers expect its effects to be observable as soon as doInTxn returns
        try {
            AuthContext.runAsSystem {
                dispose(transaction.getId())
            }
        } catch (e: Throwable) {
            if (failure == null) {
                throw e
            }
            failure.addSuppressed(e)
        }
        notifyReadOnlyParticipants(transaction, participants)
    }

    /**
     * Tells every remote participant of a read-only root to release its branch, one fire-and-forget
     * task per participant on the txn-actions executor: the root never waits for a peer, and a slow
     * peer does not delay the others. Each task runs with the root as the current transaction, so the
     * web layer sends the dispose to the replica pinned by the root's calls, with the root's id, and
     * the participant releases the branch that served the root. Read-only disposal is idempotent and
     * writes nothing to the 2PC repo, so the commit coordinator is skipped. Never throws: a
     * notification that is lost, rejected or failed is covered by the participant's TxnManagerJob.
     */
    private fun notifyReadOnlyParticipants(transaction: ManagedTransaction, participants: Collection<String>) {
        val txnId = transaction.getId()
        try {
            for (app in participants.filter { it != webAppProps.appName }) {
                txnActionsExecutor.execute("ecos-txn-ro-dispose-$txnId-$app") {
                    doWithCurrentTxn(transaction) {
                        AuthContext.runAsSystem {
                            remoteClient.disposeTxn(app, txnId)
                        }
                    }
                }.catch { e ->
                    logError(txnId, e) { "Async dispose of RO txn failed for app $app" }
                }
            }
        } catch (e: Throwable) {
            logError(txnId, e) { "Participants of RO txn were not notified" }
        }
    }

    private fun <T> doWithinTxn(
        transaction: ManagedTransaction,
        readOnly: Boolean,
        ctx: TxnManagerContext,
        action: () -> T
    ): T {
        return doWithCurrentTxn(transaction) {
            transaction.doWithinTxn(ctx, readOnly, action)
        }
    }

    /**
     * Runs [action] with [transaction] as the current transaction of the thread: thread-local and
     * MDC only, the transaction itself is not entered, so a completed or disposed one is fine. The
     * completion calls made off the thread of a transaction run this way: the web layer then sends
     * them to the replicas pinned by the transaction's calls, with the transaction id, as it does
     * for the calls made from that thread.
     */
    internal fun <T> doWithCurrentTxn(transaction: ManagedTransaction, action: () -> T): T {
        val prevTxn = currentTxn.get()
        currentTxn.set(transaction)
        var mdcTxnIdBefore: String? = null
        try {
            mdcTxnIdBefore = MDC.get(MDC_TXN_ID_KEY)
            MDC.put(MDC_TXN_ID_KEY, transaction.getId().toString())
            return action.invoke()
        } finally {
            if (prevTxn == null) {
                currentTxn.remove()
            } else {
                currentTxn.set(prevTxn)
            }
            if (mdcTxnIdBefore == null) {
                MDC.remove(MDC_TXN_ID_KEY)
            } else {
                MDC.put(MDC_TXN_ID_KEY, mdcTxnIdBefore)
            }
        }
    }

    private fun <T> doWithoutTxn(action: () -> T): T {
        val prevTxn = currentTxn.get()
        currentTxn.remove()
        var mdcTxnIdBefore: String? = null
        return try {
            mdcTxnIdBefore = MDC.get(MDC_TXN_ID_KEY)
            MDC.put(MDC_TXN_ID_KEY, "no-txn")
            action.invoke()
        } finally {
            if (prevTxn != null) {
                currentTxn.set(prevTxn)
            }
            if (mdcTxnIdBefore == null) {
                MDC.remove(MDC_TXN_ID_KEY)
            } else {
                MDC.put(MDC_TXN_ID_KEY, mdcTxnIdBefore)
            }
        }
    }

    private inline fun logError(txnId: TxnId, e: Throwable, crossinline message: () -> String) {
        log.error(e) { "[$txnId] " + message.invoke() }
    }

    private inline fun logDebug(transaction: Transaction, crossinline message: () -> String) {
        logDebug(transaction.getId(), message)
    }

    private inline fun logDebug(txnId: TxnId, crossinline message: () -> String) {
        log.debug { "[$txnId] " + message() }
    }

    override fun getCurrentTransaction(): Transaction? {
        return currentTxn.get()
    }

    internal fun getCurrentManagedTransaction(): ManagedTransaction? {
        return currentTxn.get()
    }

    override fun prepareCommitFromExtManager(txnId: TxnId, managerCanRecoverPreparedTxn: Boolean): List<EcosXid> {
        // look the context up first: the tombstone is written before the aborted context can be removed,
        // so a context the web layer recreated after the abort is never found without its tombstone
        val txnInfo = transactionsById[txnId]
        check(!abortedBranches.contains(txnId)) { "Transaction '$txnId' was rolled back before prepare" }
        checkNotNull(txnInfo) { "Transaction is not found: '$txnId'" }
        txnInfo.managerCanRecoverPreparedTxn = managerCanRecoverPreparedTxn
        val result = txnInfo.transaction.prepareCommit()
        if (result.isEmpty()) {
            dispose(txnId)
        }
        return result
    }

    override fun getManagedTransaction(txnId: TxnId): ManagedTransaction {
        return getManagedTransactionOrNull(txnId) ?: error("Transaction is not found: '$txnId'")
    }

    override fun getManagedTransactionOrNull(txnId: TxnId): ManagedTransaction? {
        return transactionsById[txnId]?.transaction
    }

    override fun getTransactionOrNull(txnId: TxnId): Transaction? {
        return transactionsById[txnId]?.transaction
    }

    override fun dispose(txnId: TxnId) {
        val info = transactionsById[txnId] ?: return
        val status = info.transaction.getStatus()
        if (status == TransactionStatus.COMMITTED || status == TransactionStatus.ROLLED_BACK) {
            recoveryManager.rememberCompleted(
                txnId,
                info.transaction.getResources().map { it.getXid() },
                status == TransactionStatus.COMMITTED
            )
        }
        info.transaction.dispose()
        transactionsById.remove(txnId, info)
    }

    /**
     * Withdraws the vote of the local branch if its prepare has not started: the coordinator
     * can no longer commit the transaction.
     */
    internal fun abortUnpreparedBranch(txnId: TxnId, cause: Throwable): Boolean {
        val transaction = transactionsById[txnId]?.transaction ?: return false
        // under the same monitor as the rollback: a concurrent dispose cannot remove the context,
        // and the web layer cannot recreate it, until the tombstone is in place
        synchronized(transaction) {
            if (!transaction.rollbackIfNotPrepared(cause)) {
                return false
            }
            abortedBranches.add(txnId)
        }
        return true
    }

    /**
     * Disposes a context that the inactivity job found stuck. The job checks the status without
     * the transaction's monitor, so a branch prepared since then stays with its coordinator.
     */
    internal fun disposeInactive(txnId: TxnId) {
        val info = transactionsById[txnId] ?: return
        val transaction = info.transaction
        // a read-only or empty branch votes the same from a recreated context, so it needs no tombstone
        if (!transaction.isReadOnly() &&
            !transaction.isEmpty() &&
            abortUnpreparedBranch(txnId, RuntimeException("Inactive transaction disposing. TxnId: $txnId"))
        ) {
            log.error { "[$txnId] Uncompleted inactive transaction rolled back before prepare" }
        }
        if (info.awaitsCoordinator()) {
            logDebug(txnId) { "Inactive transaction was prepared meanwhile and stays with its coordinator" }
            return
        }
        dispose(txnId)
    }

    override fun getStatus(txnId: TxnId): TransactionStatus {
        return transactionsById[txnId]?.transaction?.getStatus() ?: TransactionStatus.NO_TRANSACTION
    }

    override fun recoveryCommit(txnId: TxnId, xids: Collection<EcosXid>) {
        val txn = getManagedTransactionOrNull(txnId)
        if (txn != null) {
            txn.commitPrepared()
        } else {
            recoveryManager.commitPrepared(xids)
        }
    }

    override fun recoveryRollback(txnId: TxnId, xids: Collection<EcosXid>) {
        val txn = getManagedTransactionOrNull(txnId)
        if (txn != null) {
            txn.rollback(null)
        } else {
            recoveryManager.rollbackPrepared(xids)
        }
    }

    override fun getRecoveryManager(): RecoveryManager {
        return recoveryManager
    }

    override fun shutdown() {
        txnManagerJob.stop()
    }

    internal class TransactionInfo(
        val transaction: ManagedTransaction,
        var managerCanRecoverPreparedTxn: Boolean = false,
        var lastAliveTime: Long = System.currentTimeMillis(),
        var commitDelegatedToApp: String = ""
    ) {
        /**
         * A timeout cannot reverse an unresolved coordinator decision.
         * Completed contexts can release resources after caching their outcome.
         */
        fun awaitsCoordinator(): Boolean {
            return transaction.getStatus() == TransactionStatus.COMMITTING ||
                (managerCanRecoverPreparedTxn && !transaction.isCompleted())
        }
    }
}
