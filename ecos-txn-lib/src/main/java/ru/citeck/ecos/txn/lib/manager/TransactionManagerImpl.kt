package ru.citeck.ecos.txn.lib.manager

import mu.KotlinLogging
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.micrometer.EcosMicrometerContext
import ru.citeck.ecos.txn.lib.action.TxnActionId
import ru.citeck.ecos.txn.lib.action.TxnActionRef
import ru.citeck.ecos.txn.lib.action.TxnActionType
import ru.citeck.ecos.txn.lib.commit.CommitCoordinator
import ru.citeck.ecos.txn.lib.commit.CommitCoordinatorImpl
import ru.citeck.ecos.txn.lib.commit.TxnCommitData
import ru.citeck.ecos.txn.lib.commit.repo.NoopTwoPhaseCommitRepo
import ru.citeck.ecos.txn.lib.commit.repo.TwoPhaseCommitRepo
import ru.citeck.ecos.txn.lib.manager.action.TxnActionsContainer
import ru.citeck.ecos.txn.lib.manager.action.TxnActionsManager
import ru.citeck.ecos.txn.lib.manager.recovery.RecoveryManager
import ru.citeck.ecos.txn.lib.manager.recovery.RecoveryManagerImpl
import ru.citeck.ecos.txn.lib.resource.CommitPrepareStatus
import ru.citeck.ecos.txn.lib.resource.TransactionResource
import ru.citeck.ecos.txn.lib.transaction.*
import ru.citeck.ecos.txn.lib.transaction.ctx.TxnManagerContext
import ru.citeck.ecos.txn.lib.transaction.xid.EcosXid
import ru.citeck.ecos.webapp.api.EcosWebAppApi
import ru.citeck.ecos.webapp.api.apps.EcosRemoteWebAppsApi
import ru.citeck.ecos.webapp.api.properties.EcosWebAppProps
import ru.citeck.ecos.webapp.api.task.executor.EcosTaskExecutorApi
import ru.citeck.ecos.webapp.api.web.client.EcosWebClientApi
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import kotlin.collections.LinkedHashSet

class TransactionManagerImpl : TransactionManager {

    companion object {
        private val log = KotlinLogging.logger {}
    }

    internal lateinit var props: EcosTxnProps
    internal lateinit var webAppApi: EcosWebAppApi
    internal lateinit var micrometerContext: EcosMicrometerContext
    internal lateinit var actionsManager: TxnActionsManager
    internal lateinit var commitCoordinator: CommitCoordinator

    private lateinit var webAppProps: EcosWebAppProps
    private lateinit var txnActionsExecutor: EcosTaskExecutorApi
    private lateinit var webClientApi: EcosWebClientApi
    private lateinit var remoteWebAppsApi: EcosRemoteWebAppsApi
    private lateinit var txnManagerJob: TxnManagerJob
    private lateinit var recoveryManager: RecoveryManager

    private val currentTxn = ThreadLocal<ManagedTransaction>()
    internal val transactionsById = ConcurrentHashMap<TxnId, TransactionInfo>()

    @JvmOverloads
    fun init(
        webAppApi: EcosWebAppApi,
        props: EcosTxnProps,
        micrometerContext: EcosMicrometerContext = EcosMicrometerContext.NOOP,
        twoPhaseCommitRepo: TwoPhaseCommitRepo = NoopTwoPhaseCommitRepo
    ) {
        this.props = props
        this.webAppApi = webAppApi
        this.webAppProps = webAppApi.getProperties()
        this.txnActionsExecutor = webAppApi.getTasksApi().getExecutor("txn-actions")
        this.webClientApi = webAppApi.getWebClientApi()
        this.remoteWebAppsApi = webAppApi.getRemoteWebAppsApi()

        this.micrometerContext = micrometerContext
        this.actionsManager = TxnActionsManager(this)
        this.commitCoordinator = CommitCoordinatorImpl(twoPhaseCommitRepo, this)
        this.txnManagerJob = TxnManagerJob(this)
        this.recoveryManager = RecoveryManagerImpl()

        webAppApi.doWhenAppReady { this.txnManagerJob.start() }
    }

    override fun coordinateCommit(txnId: TxnId, data: TxnCommitData, txnLevel: Int) {
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
        action: () -> T
    ): T {

        return when (policy) {
            TransactionPolicy.NOT_SUPPORTED -> doWithoutTxn(action)
            TransactionPolicy.REQUIRES_NEW -> doInNewTxn(readOnly, action)
            TransactionPolicy.SUPPORTS -> {
                if (extTxnId.isEmpty()) {
                    action.invoke()
                } else {
                    doInExtTxn(extTxnId, readOnly, extCtx, action)
                }
            }

            TransactionPolicy.REQUIRED -> {
                if (extTxnId.isEmpty()) {
                    doInNewTxn(readOnly) { action.invoke() }
                } else {
                    doInExtTxn(extTxnId, readOnly, extCtx, action)
                }
            }
        }
    }

    private fun <T> doInExtTxn(extTxnId: TxnId, readOnly: Boolean, ctx: TxnManagerContext, action: () -> T): T {

        var isNewLocalTxn = false

        val transaction = transactionsById.computeIfAbsent(extTxnId) {
            isNewLocalTxn = true
            val txn = TransactionImpl(it, webAppProps.appName, readOnly)
            txn.start()
            TransactionInfo(txn)
        }.transaction

        return try {
            doWithinTxn(transaction, readOnly, ctx, action)
        } finally {
            if (isNewLocalTxn && (transaction.isEmpty() || readOnly)) {
                try {
                    dispose(extTxnId)
                } catch (e: Throwable) {
                    log.error(e) { "[${transaction.getId()}] Error while dispose local-external transaction" }
                }
            }
        }
    }

    private fun <T> doInNewTxn(readOnly: Boolean, action: () -> T): T {
        return doInNewTxn(readOnly, 0, action)
    }

    internal fun <T> doInNewTxn(readOnly: Boolean, txnLevel: Int, action: () -> T): T {

        log.debug { "Do in new txn called. ReadOnly: $readOnly level: $txnLevel" }

        val transactionStartTime = System.currentTimeMillis()

        if (txnLevel >= 10) {
            error("Transaction actions level overflow error")
        }
        val newTxnId = TxnId.create(webAppProps.appName, webAppProps.appInstanceId)
        val transaction = TransactionImpl(newTxnId, webAppProps.appName, readOnly)

        val actions = TxnActionsContainer(newTxnId, this)
        val remoteXids = LinkedHashMap<String, MutableSet<EcosXid>>()
        val localXids = LinkedHashSet<EcosXid>()

        val txnManagerContext = object : TxnManagerContext {
            override fun registerAction(type: TxnActionType, actionRef: TxnActionRef) {
                actions.addAction(type, actionRef)
            }

            override fun addRemoteXids(appName: String, xids: Set<EcosXid>) {
                remoteXids.computeIfAbsent(appName) { LinkedHashSet() }.addAll(xids)
            }

            override fun onResourceAdded(resource: TransactionResource) {
                localXids.add(resource.getXid())
            }
        }

        val result = doWithinTxn(transaction, readOnly, txnManagerContext) {

            var afterRollbackActions: List<TxnActionId> = emptyList()
            try {
                transactionsById[newTxnId] = TransactionInfo(transaction)
                transaction.start()

                val txnActionObservation = micrometerContext.createObservation("ecos.txn.action")
                    .highCardinalityKeyValue("txnId") { newTxnId.toString() }

                val actionRes = txnActionObservation.observe { action.invoke() }
                afterRollbackActions = actions.drainActions(TxnActionType.AFTER_ROLLBACK)
                val afterCommitActions = actions.drainActions(TxnActionType.AFTER_COMMIT)

                if (localXids.isNotEmpty()) {
                    remoteXids[webAppProps.appName] = localXids
                }
                if (readOnly) {
                    AuthContext.runAsSystem {
                        commitCoordinator.disposeRoot(newTxnId, remoteXids.keys, null)
                    }
                    actionRes
                } else {
                    AuthContext.runAsSystem {
                        actions.executeBeforeCommitActions()
                        val twopcActions = EnumMap<TxnActionType, List<TxnActionId>>(TxnActionType::class.java)
                        if (afterCommitActions.isNotEmpty()) {
                            twopcActions[TxnActionType.AFTER_COMMIT] = afterCommitActions
                        }
                        if (afterRollbackActions.isNotEmpty()) {
                            twopcActions[TxnActionType.AFTER_ROLLBACK] = afterRollbackActions
                        }
                        val twopcCommitData = TxnCommitData(remoteXids, twopcActions)
                        commitCoordinator.commitRoot(newTxnId, twopcCommitData, txnLevel)

                        actionRes
                    }
                }
            } catch (error: Throwable) {
                AuthContext.runAsSystem {
                    if (readOnly) {
                        commitCoordinator.disposeRoot(newTxnId, remoteXids.keys, error)
                    } else {
                        commitCoordinator.rollbackRoot(newTxnId, remoteXids.keys, afterRollbackActions, error, txnLevel)
                    }
                }
                throw error
            } finally {
                transactionsById.remove(newTxnId)
            }
        }

        val totalTime = System.currentTimeMillis() - transactionStartTime
        debug(transaction) { "Do in new txn finished. ReadOnly: $readOnly level: $txnLevel. Total time: $totalTime ms" }

        return result
    }

    private fun <T> doWithinTxn(
        transaction: ManagedTransaction,
        readOnly: Boolean,
        ctx: TxnManagerContext,
        action: () -> T
    ): T {
        val prevTxn = currentTxn.get()
        currentTxn.set(transaction)
        try {
            return transaction.doWithinTxn(ctx, readOnly, action)
        } finally {
            if (prevTxn == null) {
                currentTxn.remove()
            } else {
                currentTxn.set(prevTxn)
            }
        }
    }

    private fun <T> doWithoutTxn(action: () -> T): T {
        val prevTxn = currentTxn.get()
        currentTxn.remove()
        return try {
            action.invoke()
        } finally {
            if (prevTxn != null) {
                currentTxn.set(prevTxn)
            }
        }
    }

    private inline fun debug(transaction: Transaction, crossinline message: () -> String) {
        log.debug { "[${transaction.getId()}] " + message() }
    }

    override fun getCurrentTransaction(): Transaction? {
        return currentTxn.get()
    }

    override fun prepareCommitFromExtManager(txnId: TxnId, managerCanRecoverPreparedTxn: Boolean): CommitPrepareStatus {
        val txnInfo = transactionsById[txnId] ?: error("Transaction is not found: '$txnId'")
        txnInfo.managerCanRecoverPreparedTxn = managerCanRecoverPreparedTxn
        val result = txnInfo.transaction.prepareCommit()
        if (result == CommitPrepareStatus.NOTHING_TO_COMMIT) {
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
        transactionsById.remove(txnId)?.transaction?.dispose()
    }

    override fun getStatus(txnId: TxnId): TransactionStatus {
        return transactionsById[txnId]?.transaction?.getStatus() ?: TransactionStatus.NO_TRANSACTION
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
        var lastAliveTime: Long = System.currentTimeMillis()
    )
}
