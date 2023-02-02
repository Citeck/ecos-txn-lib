package ru.citeck.ecos.txn.lib.manager

import mu.KotlinLogging
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.txn.lib.action.TxnActionRef
import ru.citeck.ecos.txn.lib.action.TxnActionType
import ru.citeck.ecos.txn.lib.manager.api.TxnManagerWebExecutor
import ru.citeck.ecos.txn.lib.resource.CommitPrepareStatus
import ru.citeck.ecos.txn.lib.resource.TransactionResource
import ru.citeck.ecos.txn.lib.transaction.*
import ru.citeck.ecos.webapp.api.EcosWebAppApi
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import kotlin.concurrent.thread

class TransactionManagerImpl(webAppApi: EcosWebAppApi) : TransactionManager {

    companion object {
        private val log = KotlinLogging.logger {}

        private const val HEALTH_CHECK_PERIOD_MS = 20_000
        private const val MAX_NON_ALIVE_TIME_MS = 60_000
    }

    private val webAppProps = webAppApi.getProperties()
    private val txnActionsExecutor = webAppApi.getTasksApi().getExecutor("txn-actions")
    private val webClientApi = webAppApi.getWebClientApi()
    private val remoteWebAppsApi = webAppApi.getRemoteWebAppsApi()

    private val currentTxn = ThreadLocal<ManagedTransaction>()
    private val transactionsById = ConcurrentHashMap<TxnId, TransactionInfo>()

    init {
        val watcherThreadActive = AtomicBoolean(true)
        thread(start = true, name = "active-transactions-watcher") {
            while (watcherThreadActive.get()) {
                Thread.sleep(10000)
                if (transactionsById.isEmpty()) {
                    continue
                }
                try {
                    val transactionIds = transactionsById.keys.toList()
                    val prevHealthCheckTime = System.currentTimeMillis() - HEALTH_CHECK_PERIOD_MS
                    val nonAliveTime = System.currentTimeMillis() - MAX_NON_ALIVE_TIME_MS
                    for (txnId in transactionIds) {
                        val txnInfo = transactionsById[txnId] ?: continue
                        if (!txnInfo.transaction.isIdle()) {
                            txnInfo.lastAliveTime = System.currentTimeMillis()
                            continue
                        }
                        val lastActiveTime = txnInfo.transaction.getLastActiveTime().toEpochMilli()
                        if (txnInfo.lastAliveTime < lastActiveTime) {
                            txnInfo.lastAliveTime = lastActiveTime
                        }
                        if (txnInfo.lastAliveTime < prevHealthCheckTime && txnId.appName != webAppProps.appName) {
                            val appRef = txnId.appName + ":" + txnId.appInstanceId
                            AuthContext.runAsSystem {
                                if (remoteWebAppsApi.isAppAvailable(appRef)) {
                                    try {
                                        webClientApi.newRequest()
                                            .targetApp(appRef)
                                            .path(TxnManagerWebExecutor.PATH)
                                            .header(TxnManagerWebExecutor.HEADER_TXN_ID, txnId)
                                            .header(
                                                TxnManagerWebExecutor.HEADER_TYPE,
                                                TxnManagerWebExecutor.TYPE_GET_STATUS
                                            )
                                            .execute {
                                                val status = it.getBodyReader()
                                                    .readDto(TxnManagerWebExecutor.GetStatusResp::class.java).status

                                                if (status == TransactionStatus.NO_TRANSACTION) {
                                                    txnInfo.lastAliveTime = 0L
                                                } else {
                                                    txnInfo.lastAliveTime = System.currentTimeMillis()
                                                }
                                            }.get()
                                    } catch (e: Throwable) {
                                        log.warn(e) { "[$txnId] Exception while transaction status checking" }
                                    }
                                }
                            }
                        }
                        if (txnInfo.lastAliveTime < nonAliveTime) {
                            log.info { "[$txnId] Dispose stuck transaction" }
                            try {
                                dispose(txnId)
                            } catch (e: Throwable) {
                                log.error(e) { "[$txnId] Error while disposing of stuck transaction" }
                            }
                        }
                    }
                } catch (e: Throwable) {
                    if (e is InterruptedException) {
                        Thread.currentThread().interrupt()
                        throw e
                    }
                    log.error(e) { "Exception in active-transactions-watcher" }
                }
            }
        }
        Runtime.getRuntime().addShutdownHook(
            thread(start = false) {
                watcherThreadActive.set(false)
            }
        )
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
            val txn = TransactionImpl(it, webAppProps.appName)
            txn.start()
            TransactionInfo(txn)
        }.transaction

        var success = true
        val result = try {
            doWithinTxn(transaction, readOnly, ctx, action)
        } catch (e: Throwable) {
            success = false
            throw e
        } finally {
            if (isNewLocalTxn && (transaction.isEmpty() || readOnly)) {
                transaction.doWithinTxn(ctx, readOnly) {
                    try {
                        if (success) {
                            transaction.onePhaseCommit()
                        } else {
                            transaction.rollback()
                        }
                    } catch (e: Throwable) {
                        log.error(e) { "[${transaction.getId()}] Error while complete local-external transaction" }
                    }
                    try {
                        transaction.dispose()
                    } catch (e: Throwable) {
                        log.error(e) { "[${transaction.getId()}] Error while dispose local-external transaction" }
                    }
                }
                transactionsById.remove(extTxnId)
            }
        }
        return result
    }

    private fun <T> doInNewTxn(readOnly: Boolean, action: () -> T): T {
        return doInNewTxn(readOnly, 0, action)
    }

    private fun <T> doInNewTxn(readOnly: Boolean, level: Int, action: () -> T): T {
        log.debug { "Do in new txn called. ReadOnly: $readOnly level: $level" }
        if (level >= 10) {
            error("Transaction actions level overflow error")
        }
        val newTxnId = TxnId.create(webAppProps.appName, webAppProps.appInstanceId)
        val actions: MutableMap<TxnActionType, TreeSet<TxnActionRefWithIdx>> = EnumMap(TxnActionType::class.java)
        val index = AtomicInteger(0)
        val txnManagerContext = object : TxnManagerContext {
            override fun registerAction(type: TxnActionType, actionRef: TxnActionRef) {
                log.info { "Register new action: $type $actionRef" }
                actions.computeIfAbsent(type) { TreeSet() }.add(
                    TxnActionRefWithIdx(actionRef, index.getAndIncrement())
                )
            }
            override fun addResource(resource: TransactionResource): Boolean {
                return false
            }
        }
        val transaction = TransactionImpl(newTxnId, webAppProps.appName)
        return doWithinTxn(transaction, readOnly, txnManagerContext) {
            try {
                transactionsById[newTxnId] = TransactionInfo(transaction)
                transaction.start()
                val actionRes = action.invoke()
                AuthContext.runAsSystem {
                    if (!readOnly) {
                        actions[TxnActionType.BEFORE_COMMIT]?.forEach {
                            log.debug { "[${transaction.getId()}] Execute action with id: ${it.getId()}" }
                            executeAction(transaction, it.ref)
                        }
                    }
                    transaction.onePhaseCommit()
                    if (!readOnly) {
                        actions[TxnActionType.AFTER_COMMIT]?.forEach { actionRefWithIdx ->
                            doInNewTxn(false, level + 1) {
                                try {
                                    executeAction(transaction, actionRefWithIdx.ref)
                                } catch (mainError: Throwable) {
                                    log.error(mainError) { "After commit action execution error. Id: ${actionRefWithIdx.getId()}" }
                                }
                            }
                        }
                    }
                    transaction.dispose()
                    actionRes
                }
            } catch (mainError: Throwable) {
                AuthContext.runAsSystem {
                    try {
                        transaction.rollback().forEach {
                            mainError.addSuppressed(it)
                        }
                    } catch (rollbackError: Throwable) {
                        mainError.addSuppressed(rollbackError)
                    }
                    if (!readOnly) {
                        actions[TxnActionType.AFTER_ROLLBACK]?.forEach { actionRefWithIdx ->
                            doInNewTxn(false, level + 1) {
                                try {
                                    executeAction(transaction, actionRefWithIdx.ref)
                                } catch (afterRollbackActionErr: Throwable) {
                                    mainError.addSuppressed(
                                        RuntimeException(
                                            "After rollback action execution error. " +
                                                "TxnId: ${transaction.getId()} Id: ${actionRefWithIdx.getId()}",
                                            afterRollbackActionErr
                                        )
                                    )
                                }
                            }
                        }
                    }
                    try {
                        transaction.dispose()
                    } catch (disposeErr: Throwable) {
                        mainError.addSuppressed(
                            RuntimeException(
                                "Error while disposing of ${transaction.getId()}",
                                disposeErr
                            )
                        )
                    }
                    throw mainError
                }
            } finally {
                transactionsById.remove(newTxnId)
            }
        }
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

    override fun executeAction(txnId: TxnId, actionId: Int) {
        getRequiredExtTxn(txnId).executeAction(actionId)
    }

    private fun executeAction(transaction: Transaction, actionRef: TxnActionRef) {
        if (actionRef.appName == webAppProps.appName) {
            transaction.executeAction(actionRef.id)
        } else {
            AuthContext.runAsSystem {
                webClientApi.newRequest()
                    .targetApp(actionRef.appName)
                    .path(TxnManagerWebExecutor.PATH)
                    .header(TxnManagerWebExecutor.HEADER_TXN_ID, transaction.getId())
                    .header(TxnManagerWebExecutor.HEADER_TYPE, TxnManagerWebExecutor.TYPE_EXEC_ACTION)
                    .header(TxnManagerWebExecutor.HEADER_ACTION_ID, actionRef.id)
                    .execute {}.get()
            }
        }
    }

    override fun prepareCommit(txnId: TxnId): CommitPrepareStatus {
        return getRequiredExtTxn(txnId).prepareCommit()
    }

    override fun commitPrepared(txnId: TxnId) {
        return getRequiredExtTxn(txnId).commitPrepared()
    }

    override fun onePhaseCommit(txnId: TxnId) {
        return getRequiredExtTxn(txnId).onePhaseCommit()
    }

    override fun rollback(txnId: TxnId) {
        getRequiredExtTxn(txnId).rollback()
    }

    override fun getCurrentTransaction(): Transaction? {
        return currentTxn.get()
    }

    private fun getRequiredExtTxn(txnId: TxnId): ManagedTransaction {
        return transactionsById[txnId]?.transaction ?: error("Transaction is not found: '$txnId'")
    }

    override fun dispose(txnId: TxnId) {
        transactionsById.remove(txnId)?.transaction?.dispose()
    }

    override fun getStatus(txnId: TxnId): TransactionStatus {
        return transactionsById[txnId]?.transaction?.getStatus() ?: TransactionStatus.NO_TRANSACTION
    }

    private class TxnActionRefWithIdx(val ref: TxnActionRef, val index: Int) : Comparable<TxnActionRefWithIdx> {

        override fun compareTo(other: TxnActionRefWithIdx): Int {
            val res = ref.order.compareTo(other.ref.order)
            if (res != 0) {
                return res
            }
            return index.compareTo(other.index)
        }

        fun getId(): Int {
            return ref.id
        }
    }

    private class TransactionInfo(
        val transaction: ManagedTransaction,
        var lastAliveTime: Long = System.currentTimeMillis()
    )
}
