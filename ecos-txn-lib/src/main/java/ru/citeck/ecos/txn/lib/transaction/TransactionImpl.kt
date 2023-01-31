package ru.citeck.ecos.txn.lib.transaction

import mu.KotlinLogging
import ru.citeck.ecos.txn.lib.action.TxnActionRef
import ru.citeck.ecos.txn.lib.action.TxnActionType
import ru.citeck.ecos.txn.lib.resource.CommitPrepareStatus
import ru.citeck.ecos.txn.lib.resource.TransactionResource
import java.lang.RuntimeException
import java.util.*
import java.util.concurrent.atomic.AtomicInteger
import kotlin.collections.ArrayList
import kotlin.collections.HashMap
import kotlin.collections.LinkedHashMap

class TransactionImpl(
    private val txnId: TxnId,
    private val appName: String
) : ManagedTransaction {

    companion object {
        private val log = KotlinLogging.logger {}
    }

    private val actionCounter = AtomicInteger(0)

    private val resources = ArrayList<ResourceData>()
    private val resourcesByKey = HashMap<Any, ResourceData>()

    private val actionsById: MutableMap<Int, ActionData> = HashMap()

    private var txnManagerContext: TxnManagerContext? = null

    private var txnStatus: TransactionStatus = TransactionStatus.NEW

    private var readOnly = false

    private val synchronizations = ArrayList<TransactionSynchronization>()

    private val data = LinkedHashMap<Any, Any?>()

    override fun start() {
        debug { "txn start" }
        checkStatus(TransactionStatus.NEW)
        setStatus(TransactionStatus.ACTIVE)
    }

    override fun <K : Any, T : TransactionResource> getOrAddRes(key: K, resource: (K, TxnId) -> T): T {
        debug { "getOrAddRes with key $key" }
        checkStatus(TransactionStatus.ACTIVE)
        val currentResource = resourcesByKey[key]
        val resourceRes = if (currentResource != null) {
            currentResource.resource
        } else {
            debug { "Create new resource for key $key" }
            val res = resource.invoke(key, txnId)
            res.start()
            if (!txnManagerContext!!.addResource(res)) {
                val data = ResourceData(res, res.getName())
                resources.add(data)
                resourcesByKey[key] = data
            }
            res
        }
        @Suppress("UNCHECKED_CAST")
        return resourceRes as T
    }

    override fun executeAction(actionId: Int) {
        debug { "Execute action $actionId" }
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
        try {
            return doWithReadOnlyFlag(readOnly, action)
        } finally {
            this.txnManagerContext = prevContext
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
        debug { "Register action $type - $action" }
        val context = this.txnManagerContext ?: error("doWithinTxnContext is null")
        context.registerAction(type, action)
    }

    override fun addAction(type: TxnActionType, order: Float, async: Boolean, action: () -> Unit) {
        checkStatus(TransactionStatus.ACTIVE)
        val actionId = actionCounter.getAndIncrement()
        actionsById[actionId] = ActionData(async, action)
        addAction(type, TxnActionRef(appName, actionId, order))
    }

    override fun prepareCommit(): CommitPrepareStatus {
        debug { "Prepare commit" }
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
        debug { "Commit prepared" }
        checkStatus(TransactionStatus.PREPARED)
        setStatus(TransactionStatus.COMMITTING)
        for (resData in resources) {
            resData.resource.commitPrepared()
            resData.committed = true
        }
        setStatus(TransactionStatus.COMMITTED)
    }

    override fun onePhaseCommit() {
        debug { "One phase commit" }
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

    override fun rollback(): List<Throwable> {
        debug { "Rollback" }
        checkStatus(
            TransactionStatus.ACTIVE,
            TransactionStatus.PREPARING,
            TransactionStatus.PREPARED,
            TransactionStatus.COMMITTING
        )
        setStatus(TransactionStatus.ROLLING_BACK)
        val errorsRes = mutableListOf<Throwable>()
        for (resData in resources) {
            if (!resData.committed) {
                try {
                    resData.resource.rollback()
                } catch (e: Throwable) {
                    errorsRes.add(RuntimeException("Error while rollback. Resource: '${resData.name}'", e))
                }
            } else {
                log.error { "Resource already committed and can't be rolled back: '${resData.name}'" }
            }
        }
        setStatus(TransactionStatus.ROLLED_BACK)
        return errorsRes
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
            log.error("[$txnId] Uncompleted transaction disposing. Status: $txnStatus")
            try {
                rollback()
            } catch (e: Throwable) {
                log.debug(e) { "Exception while rollback in dispose method. Status: $txnStatus" }
            }
        }
        for (resData in resources) {
            try {
                resData.resource.dispose()
            } catch (e: Throwable) {
                log.error(e) { "Exception while disposing" }
            }
        }
        setStatus(TransactionStatus.DISPOSED)
    }

    private inline fun debug(crossinline msg: () -> String) {
        log.debug { "[" + txnId + "] " + msg.invoke() }
    }

    private inline fun error(e: Throwable, crossinline msg: () -> String) {
        log.error(e) { "[" + txnId + "] " + msg.invoke() }
    }

    override fun getStatus(): TransactionStatus {
        return txnStatus
    }

    private fun setStatus(newStatus: TransactionStatus) {
        if (newStatus == this.txnStatus) {
            return
        }
        if (newStatus == TransactionStatus.PREPARING || newStatus == TransactionStatus.COMMITTING) {
            synchronizations.forEach {
                it.beforeCompletion()
            }
        }
        debug { "Update status ${this.txnStatus} -> $newStatus" }
        val statusBefore = this.txnStatus
        this.txnStatus = newStatus
        if (this.txnStatus == TransactionStatus.ROLLED_BACK || this.txnStatus == TransactionStatus.COMMITTED) {
            synchronizations.forEach {
                try {
                    it.afterCompletion(this.txnStatus)
                } catch (e: Throwable) {
                    log.error(e) { "[$txnId] Exception in sync.afterCompletion call" }
                }
            }
        }
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
        val async: Boolean,
        val action: () -> Unit,
        var executed: Boolean = false
    )

    private class ResourceData(
        val resource: TransactionResource,
        val name: String,
        var committed: Boolean = false
    )
}
