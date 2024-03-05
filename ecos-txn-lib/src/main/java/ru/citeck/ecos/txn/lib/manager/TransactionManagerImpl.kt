package ru.citeck.ecos.txn.lib.manager

import mu.KotlinLogging
import org.slf4j.MDC
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
import ru.citeck.ecos.txn.lib.manager.api.client.CurrentAppClientWrapper
import ru.citeck.ecos.txn.lib.manager.api.client.TxnManagerRemoteApiClient
import ru.citeck.ecos.txn.lib.manager.api.client.TxnManagerWebApiClient
import ru.citeck.ecos.txn.lib.manager.recovery.RecoveryManager
import ru.citeck.ecos.txn.lib.manager.recovery.RecoveryManagerImpl
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
    private lateinit var recoveryManager: RecoveryManager

    private val currentTxn = ThreadLocal<ManagedTransaction>()
    internal val transactionsById = ConcurrentHashMap<TxnId, TransactionInfo>()

    @JvmOverloads
    fun init(
        webAppApi: EcosWebAppApi,
        props: EcosTxnProps = EcosTxnProps(),
        micrometerContext: EcosMicrometerContext = EcosMicrometerContext.NOOP,
        twoPhaseCommitRepo: TwoPhaseCommitRepo = NoopTwoPhaseCommitRepo,
        remoteClient: TxnManagerRemoteApiClient? = null
    ) {
        this.props = props
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
        action: (ExtTxnWorkContext) -> T
    ): T {

        return when (policy) {
            TransactionPolicy.NOT_SUPPORTED -> doWithoutTxn { action(AlwaysStatelessExtTxnWorkContext) }
            TransactionPolicy.REQUIRES_NEW -> doInNewTxn(readOnly) { action(AlwaysStatelessExtTxnWorkContext) }
            TransactionPolicy.SUPPORTS -> {
                if (extTxnId.isEmpty()) {
                    action.invoke(AlwaysStatelessExtTxnWorkContext)
                } else {
                    doInExtTxn(extTxnId, readOnly, extCtx, action)
                }
            }

            TransactionPolicy.REQUIRED -> {
                if (extTxnId.isEmpty()) {
                    doInNewTxn(readOnly) { action.invoke(AlwaysStatelessExtTxnWorkContext) }
                } else {
                    doInExtTxn(extTxnId, readOnly, extCtx, action)
                }
            }
        }
    }

    private fun <T> doInExtTxn(
        extTxnId: TxnId,
        readOnly: Boolean,
        ctx: TxnManagerContext,
        action: (ExtTxnWorkContext) -> T
    ): T {

        var isNewLocalTxn = false

        val transaction = transactionsById.computeIfAbsent(extTxnId) {
            isNewLocalTxn = true
            val txn = TransactionImpl(it, webAppProps.appName, readOnly)
            txn.start()
            TransactionInfo(txn)
        }.transaction

        val workContext = object : ExtTxnWorkContext {
            override fun stopWork(): Boolean {
                val workWasStateless = isNewLocalTxn && (transaction.isEmpty() || readOnly)
                if (workWasStateless) {
                    try {
                        dispose(extTxnId)
                    } catch (e: Throwable) {
                        logError(extTxnId, e) { "Error while dispose local-external transaction" }
                    }
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

        log.debug {
            "Do in new txn called. " +
                "ReadOnly: $readOnly " +
                "level: $txnLevel " +
                "currentTxn: ${currentTxn.get()?.getId()}"
        }

        val transactionStartTime = System.currentTimeMillis()

        if (txnLevel >= 10) {
            error("Transaction actions level overflow error")
        }
        val newTxnId = TxnId.create(webAppProps.appName, webAppProps.appInstanceId)
        val transaction = TransactionImpl(newTxnId, webAppProps.appName, readOnly)

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

            try {
                transactionsById[newTxnId] = TransactionInfo(transaction)
                transaction.start()

                val txnActionObservation = micrometerContext.createObservation("ecos.txn.action")
                    .highCardinalityKeyValue("txnId") { newTxnId.toString() }

                val actionRes = txnActionObservation.observe { action.invoke() }

                if (readOnly) {
                    AuthContext.runAsSystem {
                        commitCoordinator.disposeRoot(newTxnId, xidsByApp.keys, null)
                    }
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
                        commitCoordinator.commitRoot(newTxnId, twopcCommitData, txnLevel)

                        actionRes
                    }
                }
            } catch (error: Throwable) {
                AuthContext.runAsSystem {
                    if (readOnly || transaction.getStatus() == TransactionStatus.COMMITTED ||
                        transaction.getStatus() == TransactionStatus.ROLLED_BACK
                    ) {
                        // transaction may be disposed early by commit coordinator
                        if (transactionsById.contains(newTxnId)) {
                            dispose(newTxnId)
                        }
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
            } finally {
                dispose(newTxnId)
            }
        }

        val totalTime = System.currentTimeMillis() - transactionStartTime
        logDebug(transaction) { "Do in new txn finished. ReadOnly: $readOnly level: $txnLevel. Total time: $totalTime ms" }

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
        var mdcTxnIdBefore: String? = null
        try {
            mdcTxnIdBefore = MDC.get(MDC_TXN_ID_KEY)
            MDC.put(MDC_TXN_ID_KEY, transaction.getId().toString())
            return transaction.doWithinTxn(ctx, readOnly, action)
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
        log.error(e) { "[$txnId]" + message.invoke() }
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

    override fun prepareCommitFromExtManager(txnId: TxnId, managerCanRecoverPreparedTxn: Boolean): List<EcosXid> {
        val txnInfo = transactionsById[txnId] ?: error("Transaction is not found: '$txnId'")
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
