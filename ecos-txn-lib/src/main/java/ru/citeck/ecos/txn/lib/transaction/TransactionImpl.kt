package ru.citeck.ecos.txn.lib.transaction

import mu.KotlinLogging
import ru.citeck.ecos.txn.lib.action.TxnActionRef
import ru.citeck.ecos.txn.lib.action.TxnActionType
import ru.citeck.ecos.txn.lib.resource.CommitPrepareStatus
import ru.citeck.ecos.txn.lib.resource.TransactionResource
import ru.citeck.ecos.txn.lib.transaction.ctx.EmptyTxnManagerContext
import ru.citeck.ecos.txn.lib.transaction.ctx.TxnManagerContext
import ru.citeck.ecos.txn.lib.transaction.xid.EcosXid
import java.lang.RuntimeException
import java.time.Instant
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import kotlin.collections.ArrayList
import kotlin.collections.HashMap
import kotlin.collections.LinkedHashMap

class TransactionImpl(
    private val txnId: TxnId,
    private val appName: String,
    private val initialReadOnly: Boolean
) : ManagedTransaction {

    companion object {
        private val log = KotlinLogging.logger {}
    }

    private val actionCounter = AtomicInteger(0)

    private val resources = ArrayList<ResourceData>()
    private val resourcesByKey = HashMap<Any, ResourceData>()

    private val actionsById: MutableMap<Int, ActionData> = HashMap()

    private var txnManagerContext: TxnManagerContext = EmptyTxnManagerContext

    private var txnStatus: TransactionStatus = TransactionStatus.NEW

    private var readOnly = initialReadOnly

    private val synchronizations = ArrayList<TransactionSynchronization>()

    private val data = LinkedHashMap<Any, Any?>()

    private val isIdle = AtomicBoolean(true)
    private val lastActiveTime = AtomicReference(Instant.now())

    override fun start() {
        logDebug { "txn start" }
        checkStatus(TransactionStatus.NEW)
        setStatus(TransactionStatus.ACTIVE)
    }

    override fun getResources(): List<TransactionResource> {
        return resources.map { it.resource }
    }

    override fun getResourcesNames(): List<String> {
        return resources.map { it.name }
    }

    override fun addRemoteXids(appName: String, xids: Set<EcosXid>) {
        logDebug { "registerRemoteXids for app $appName xids: $xids" }
        if (txnStatus != TransactionStatus.ACTIVE) {
            error("[$txnId] Remote xids can't be added when transaction is not active. AppName: $appName xids: $xids")
        }
        txnManagerContext.addRemoteXids(appName, xids)
    }

    override fun <K : Any, T : TransactionResource> getOrAddRes(key: K, resource: (K, TxnId) -> T): T {
        logDebug { "getOrAddRes with key $key" }
        val currentResource = resourcesByKey[key]
        val resourceRes = if (currentResource != null) {
            currentResource.resource
        } else {
            if (txnStatus != TransactionStatus.ACTIVE) {
                error("[$txnId] Resource can't be added when transaction is not active. Key: $key")
            }
            logDebug { "Create new resource for key $key" }
            val res = resource.invoke(key, txnId)
            res.start()
            val data = ResourceData(res, res.getName())
            resources.add(data)
            resourcesByKey[key] = data
            txnManagerContext.onResourceAdded(res)
            res
        }
        @Suppress("UNCHECKED_CAST")
        return resourceRes as T
    }

    override fun executeAction(actionId: Int) {
        logDebug { "Execute action $actionId" }
        val action = actionsById[actionId] ?: error("Action is not found by id $actionId")
        if (action.executed) {
            error("Action with id $actionId already executed")
        }
        try {
            action.action.invoke()
        } finally {
            action.executed = true
        }
    }

    override fun <T> doWithinTxn(managerCtx: TxnManagerContext, readOnly: Boolean, action: () -> T): T {
        val prevContext = this.txnManagerContext
        this.txnManagerContext = managerCtx
        val idleBefore = isIdle.get()
        isIdle.set(false)
        lastActiveTime.set(Instant.now())
        try {
            return doWithReadOnlyFlag(readOnly, action)
        } finally {
            this.txnManagerContext = prevContext
            isIdle.set(idleBefore)
            lastActiveTime.set(Instant.now())
        }
    }

    override fun <T> doWithinTxn(readOnly: Boolean, action: () -> T): T {
        return doWithReadOnlyFlag(readOnly, action)
    }

    private inline fun <T> doWithReadOnlyFlag(readOnly: Boolean, action: () -> T): T {
        if (this.readOnly && !readOnly) {
            error("You can't execute non readOnly action inside readOnly action")
        }
        val prevValue = this.readOnly
        this.readOnly = readOnly
        try {
            return action.invoke()
        } finally {
            this.readOnly = prevValue
        }
    }

    override fun addAction(type: TxnActionType, action: TxnActionRef) {
        logDebug { "Register action $type - $action" }
        txnManagerContext.registerAction(type, action)
    }

    @Deprecated(
        "use addAction without async flag",
        replaceWith = ReplaceWith("this.addAction(type, order, action)")
    )
    override fun addAction(type: TxnActionType, order: Float, async: Boolean, action: () -> Unit) {
        addAction(type, order, action)
    }

    override fun addAction(type: TxnActionType, order: Float, action: () -> Unit) {
        checkStatus(TransactionStatus.ACTIVE)
        val actionId = actionCounter.getAndIncrement()
        actionsById[actionId] = ActionData(action)
        addAction(type, TxnActionRef(appName, actionId, order))
    }

    override fun prepareCommit(): CommitPrepareStatus {
        logDebug { "Prepare commit" }
        checkStatus(TransactionStatus.ACTIVE)
        setStatus(TransactionStatus.PREPARING)
        var status = CommitPrepareStatus.NOTHING_TO_COMMIT
        for (res in resources) {
            res.resource.end()
        }
        for (res in resources) {
            if (res.resource.prepareCommit() == CommitPrepareStatus.PREPARED) {
                status = CommitPrepareStatus.PREPARED
            }
        }
        setStatus(TransactionStatus.PREPARED)
        return status
    }

    override fun commitPrepared() {
        logDebug { "Commit prepared" }
        checkStatus(TransactionStatus.PREPARED)
        setStatus(TransactionStatus.COMMITTING)
        for (resData in resources) {
            resData.resource.commitPrepared()
            resData.committed = true
        }
        setStatus(TransactionStatus.COMMITTED)
    }

    override fun onePhaseCommit() {
        logDebug { "One phase commit" }
        checkStatus(TransactionStatus.ACTIVE)
        setStatus(TransactionStatus.COMMITTING)
        if (resources.isNotEmpty()) {
            if (resources.size == 1) {
                resources.first().let {
                    it.resource.end()
                    it.resource.onePhaseCommit()
                    it.committed = true
                }
            } else {
                for (res in resources) {
                    res.resource.end()
                }
                val resourcesToCommit = resources.filter {
                    it.resource.prepareCommit() == CommitPrepareStatus.PREPARED
                }
                for (resData in resourcesToCommit) {
                    resData.resource.commitPrepared()
                    resData.committed = true
                }
            }
        }
        setStatus(TransactionStatus.COMMITTED)
    }

    override fun rollback(cause: Throwable?) {
        logDebug { "Rollback" }
        checkStatus(
            TransactionStatus.ACTIVE,
            TransactionStatus.PREPARING,
            TransactionStatus.PREPARED,
            TransactionStatus.COMMITTING
        )
        setStatus(TransactionStatus.ROLLING_BACK)
        // val errorsRes = mutableListOf<Throwable>()
        for (resData in resources) {
            if (!resData.committed) {
                try {
                    resData.resource.rollback()
                } catch (e: Throwable) {
                    if (cause == null) {
                        throw e
                    } else {
                        cause.addSuppressed(RuntimeException("Error while rollback. Resource: '${resData.name}'", e))
                    }
                }
            } else {
                logError { "Resource already committed and can't be rolled back: '${resData.name}'" }
            }
        }
        setStatus(TransactionStatus.ROLLED_BACK)
    }

    override fun registerSync(sync: TransactionSynchronization) {
        checkStatus(TransactionStatus.ACTIVE)
        synchronizations.add(sync)
    }

    private fun checkStatus(expectedStatus: TransactionStatus) {
        if (txnStatus != expectedStatus) {
            error("Invalid status: $txnStatus")
        }
    }

    private fun checkStatus(vararg expectedStatuses: TransactionStatus) {
        if (expectedStatuses.none { txnStatus == it }) {
            error("Invalid status: $txnStatus")
        }
    }

    override fun isReadOnly(): Boolean {
        return readOnly
    }

    override fun getId(): TxnId {
        return txnId
    }

    override fun isEmpty(): Boolean {
        return resources.isEmpty() && actionsById.isEmpty()
    }

    override fun dispose() {
        if (isEmpty()) {
            setStatus(TransactionStatus.DISPOSED)
            return
        }
        if (!isCompleted()) {
            if (readOnly) {
                onePhaseCommit()
            } else {
                logError { "Uncompleted transaction disposing. Status: $txnStatus" }
                val rollbackCause = RuntimeException("Uncompleted transaction disposing. TxnId: $txnId Status: $txnStatus")
                rollback(rollbackCause)
                if (rollbackCause.suppressed.isNotEmpty()) {
                    logError(rollbackCause) { "Exceptions occurred while rollback of $txnId" }
                }
            }
        }
        for (resData in resources) {
            try {
                resData.resource.dispose()
            } catch (e: Throwable) {
                logError(e) { "Exception while disposing resource '${resData.name}'" }
            }
        }
        setStatus(TransactionStatus.DISPOSED)
    }

    private inline fun logDebug(crossinline msg: () -> String) {
        if (!initialReadOnly) {
            log.debug { "[" + txnId + "] " + msg.invoke() }
        } else {
            log.trace { "[" + txnId + "] " + msg.invoke() }
        }
    }

    private inline fun logError(e: Throwable, crossinline msg: () -> String) {
        log.error(e) { "[" + txnId + "] " + msg.invoke() }
    }
    private inline fun logError(crossinline msg: () -> String) {
        log.error { "[" + txnId + "] " + msg.invoke() }
    }

    override fun getStatus(): TransactionStatus {
        return txnStatus
    }

    private fun setStatus(newStatus: TransactionStatus) {
        if (newStatus == this.txnStatus) {
            return
        }
        lastActiveTime.set(Instant.now())
        if (newStatus == TransactionStatus.PREPARING || newStatus == TransactionStatus.COMMITTING) {
            synchronizations.forEach {
                it.beforeCompletion()
            }
        }
        logDebug { "Update status ${this.txnStatus} -> $newStatus" }
        this.txnStatus = newStatus
        if (this.txnStatus == TransactionStatus.ROLLED_BACK || this.txnStatus == TransactionStatus.COMMITTED) {
            synchronizations.forEach {
                try {
                    it.afterCompletion(this.txnStatus)
                } catch (e: Throwable) {
                    logError(e) { "Exception in sync.afterCompletion call" }
                }
            }
        }
        lastActiveTime.set(Instant.now())
    }

    override fun isIdle(): Boolean {
        return isIdle.get()
    }

    override fun getLastActiveTime(): Instant {
        return lastActiveTime.get()
    }

    override fun <T : Any> getData(key: Any): T? {
        @Suppress("UNCHECKED_CAST")
        return data[key] as? T?
    }

    override fun <K : Any, T : Any> getData(key: K, compute: (K) -> T): T {
        @Suppress("UNCHECKED_CAST")
        return data.computeIfAbsent(key) { compute.invoke(it as K) } as T
    }

    override fun isCompleted(): Boolean {
        return when (this.txnStatus) {
            TransactionStatus.ROLLED_BACK,
            TransactionStatus.COMMITTED -> true
            else -> false
        }
    }

    private class ActionData(
        val action: () -> Unit,
        var executed: Boolean = false
    )

    private class ResourceData(
        val resource: TransactionResource,
        val name: String,
        var committed: Boolean = false
    )
}
