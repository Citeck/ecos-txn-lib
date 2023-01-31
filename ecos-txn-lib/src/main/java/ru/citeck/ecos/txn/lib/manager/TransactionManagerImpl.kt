package ru.citeck.ecos.txn.lib.manager

import mu.KotlinLogging
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
    }

    private val webAppProps = webAppApi.getProperties()
    private val txnActionsExecutor = webAppApi.getTasksApi().getExecutor("txn-actions")
    private val webClientApi = webAppApi.getWebClientApi()

    private val currentTxn = ThreadLocal<ManagedTransaction>()
    private val transactionsById = ConcurrentHashMap<TxnId, ManagedTransaction>()

    init {
        val watcherThreadActive = AtomicBoolean(true)
        thread(start = true, name = "active-transactions-watcher") {
            while (watcherThreadActive.get()) {
                if (transactionsById.isNotEmpty()) {
                    log.info {
                        "Active transactions: ${transactionsById.values.map {
                            it.getId().toString() + " - " + it.getStatus() + " - isEmpty: " + it.isEmpty()
                        }}"
                    }
                }
                Thread.sleep(10000)
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
        val transaction = transactionsById.computeIfAbsent(extTxnId) {
            val txn = TransactionImpl(it, webAppProps.appName)
            txn.start()
            txn
        }
        val result = doWithinTxn(transaction, readOnly, ctx, action)
        if (transaction.isEmpty()) {
            transaction.dispose()
        }
        transactionsById.remove(extTxnId)
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
                transactionsById[newTxnId] = transaction
                transaction.start()
                val actionRes = action.invoke()
                if (!readOnly) {
                    actions[TxnActionType.BEFORE_COMMIT]?.forEach {
                        log.info { "Execute action: ${transaction.getId()} id: ${it.getId()}" }
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
            } catch (mainError: Throwable) {
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
                    mainError.addSuppressed(RuntimeException("Error while disposing of ${transaction.getId()}", disposeErr))
                }
                throw mainError
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
            webClientApi.newRequest()
                .targetApp(actionRef.appName)
                .path(TxnManagerWebExecutor.PATH)
                .header(TxnManagerWebExecutor.HEADER_TXN_ID, transaction.getId())
                .header(TxnManagerWebExecutor.HEADER_TYPE, TxnManagerWebExecutor.TYPE_EXEC_ACTION)
                .header(TxnManagerWebExecutor.HEADER_ACTION_ID, actionRef.id)
                .execute {}
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
        return transactionsById[txnId] ?: error("Transaction is not found: '$txnId'")
    }

    override fun dispose(txnId: TxnId) {
        transactionsById.remove(txnId)?.dispose()
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
}
