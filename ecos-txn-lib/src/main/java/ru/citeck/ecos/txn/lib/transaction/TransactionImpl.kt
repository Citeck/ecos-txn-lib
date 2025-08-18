package ru.citeck.ecos.txn.lib.transaction

import io.github.oshai.kotlinlogging.KotlinLogging
import ru.citeck.ecos.micrometer.EcosMicrometerContext
import ru.citeck.ecos.txn.lib.TxnContext
import ru.citeck.ecos.txn.lib.action.TxnActionRef
import ru.citeck.ecos.txn.lib.action.TxnActionType
import ru.citeck.ecos.txn.lib.resource.CommitPrepareStatus
import ru.citeck.ecos.txn.lib.resource.TransactionResource
import ru.citeck.ecos.txn.lib.transaction.ctx.EmptyTxnManagerContext
import ru.citeck.ecos.txn.lib.transaction.ctx.TxnManagerContext
import ru.citeck.ecos.txn.lib.transaction.obs.EcosTxnSyncObsContext
import ru.citeck.ecos.txn.lib.transaction.xid.EcosXid
import java.time.Instant
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import kotlin.RuntimeException
import kotlin.collections.ArrayList
import kotlin.collections.HashMap
import kotlin.collections.LinkedHashMap

class TransactionImpl(
    private val txnId: TxnId,
    private val currentAppName: String,
    initialReadOnly: Boolean,
    private val micrometerContext: EcosMicrometerContext
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

    private var baseReadOnly: Boolean = initialReadOnly
    private var readOnly = initialReadOnly

    private val synchronizations = ArrayList<TransactionSynchronization>()

    private val data = LinkedHashMap<Any, Any?>()

    private val isIdle = AtomicBoolean(true)
    private val lastActiveTime = AtomicReference(Instant.now())

    override fun setReadOnly(readOnly: Boolean) {
        this.baseReadOnly = readOnly
        this.readOnly = readOnly
    }

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

    override fun registerXids(appName: String, xids: Collection<EcosXid>) {
        logDebug { "registerRemoteXids for app $appName xids: $xids" }
        if (txnStatus != TransactionStatus.ACTIVE) {
            error("[$txnId] Remote xids can't be added when transaction is not active. AppName: $appName xids: $xids")
        }
        txnManagerContext.registerXids(appName, xids)
        updateLastActiveTime()
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
            if (res.getXid().getTransactionId() != txnId) {
                error("Resource ${res.getXid()} with name ${res.getName()} is not belongs to transaction $txnId")
            }
            res.start()
            val data = ResourceData(res, res.getName())
            resources.add(data)
            resourcesByKey[key] = data
            txnManagerContext.registerXids(currentAppName, setOf(res.getXid()))
            res
        }
        updateLastActiveTime()
        @Suppress("UNCHECKED_CAST")
        return resourceRes as T
    }

    override fun executeAction(actionId: Int) {
        logDebug { "Execute action $actionId" }
        updateLastActiveTime()
        val action = actionsById[actionId] ?: error("Action is not found by id $actionId")
        if (!action.executed.get()) {
            try {
                action.action.invoke()
            } finally {
                action.executed.set(true)
            }
        } else {
            logDebug { "Action with id $actionId already executed" }
        }
        updateLastActiveTime()
    }

    override fun <T> doWithinTxn(managerCtx: TxnManagerContext, readOnly: Boolean, action: () -> T): T {
        val prevContext = this.txnManagerContext
        this.txnManagerContext = managerCtx
        val idleBefore = isIdle.get()
        isIdle.set(false)
        updateLastActiveTime()
        try {
            return doWithReadOnlyFlag(readOnly, action)
        } finally {
            this.txnManagerContext = prevContext
            isIdle.set(idleBefore)
            updateLastActiveTime()
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
        addAction(type, TxnActionRef(currentAppName, actionId, order))
    }

    override fun prepareCommit(): List<EcosXid> {
        logDebug { "Prepare commit" }
        checkStatus(TransactionStatus.ACTIVE)
        setStatus(TransactionStatus.PREPARING)
        val preparedXids = ArrayList<EcosXid>()

        for (res in resources) {
            res.resource.end()
        }
        for (res in resources) {
            if (res.resource.prepareCommit() == CommitPrepareStatus.PREPARED) {
                preparedXids.add(res.resource.getXid())
                res.wasPreparedToCommit = true
            } else {
                res.readOnlyOnPrepare = true
            }
        }
        setStatus(TransactionStatus.PREPARED)
        if (preparedXids.isEmpty() && actionsById.isNotEmpty()) {
            preparedXids.add(EcosXid.create(txnId, ByteArray(0)))
        }
        logDebug { "Prepare commit completed with xids: $preparedXids" }
        return preparedXids
    }

    override fun commitPrepared() {
        logDebug { "Commit prepared" }
        checkStatus(TransactionStatus.PREPARED)
        setStatus(TransactionStatus.COMMITTING)
        for (resData in resources) {
            if (!resData.readOnlyOnPrepare) {
                resData.resource.commitPrepared()
            }
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
            TransactionStatus.COMMITTING,
            TransactionStatus.ROLLING_BACK
        )
        setStatus(TransactionStatus.ROLLING_BACK)
        var exception = cause
        for (resData in resources) {
            if (!resData.committed && !resData.rolledBack) {
                try {
                    resData.resource.rollback()
                    resData.rolledBack = true
                } catch (e: Throwable) {
                    val errorMsg = "Error while rollback. Txn: $txnId Resource: '${resData.name}'"
                    if (resData.wasPreparedToCommit) {
                        if (exception == null) {
                            exception = RuntimeException(errorMsg)
                        }
                        exception.addSuppressed(e)
                    } else {
                        resData.rolledBack = true
                        log.debug(e) { errorMsg }
                    }
                }
            } else if (resData.committed) {
                logDebug { "Resource already committed and can't be rolled back. Resource: '${resData.name}'" }
            } else {
                logDebug { "Resource already rolled back. Resource: '${resData.name}'" }
            }
        }
        if (cause == null && exception != null) {
            throw exception
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
            error("Invalid status: $txnStatus. Expected one of ${expectedStatuses.joinToString()}")
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
                try {
                    onePhaseCommit()
                } catch (e: Throwable) {
                    logDebug(e) { "onePhaseCommit completed with error" }
                    // error is not a problem in RO transaction
                    setStatus(TransactionStatus.COMMITTED)
                }
            } else {
                logError { "Uncompleted transaction disposing. Status: $txnStatus" }
                val rollbackCause = RuntimeException(
                    "Uncompleted transaction disposing. TxnId: $txnId Status: $txnStatus"
                )
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
        resources.clear()
        actionsById.clear()
        setStatus(TransactionStatus.DISPOSED)
    }

    private inline fun logDebug(error: Throwable, crossinline msg: () -> String) {
        if (!baseReadOnly) {
            log.debug(error) { getTxnForLog() + msg.invoke() }
        } else {
            log.trace(error) { getTxnForLog() + msg.invoke() }
        }
    }

    private inline fun logDebug(crossinline msg: () -> String) {
        if (!baseReadOnly) {
            log.debug { getTxnForLog() + msg.invoke() }
        } else {
            log.trace { getTxnForLog() + msg.invoke() }
        }
    }

    private inline fun logError(e: Throwable, crossinline msg: () -> String) {
        log.error(e) { getTxnForLog() + msg.invoke() }
    }

    private inline fun logError(crossinline msg: () -> String) {
        log.error { getTxnForLog() + msg.invoke() }
    }

    private fun getTxnForLog(): String {
        val currentTxn = TxnContext.getTxnOrNull()
        return if (currentTxn?.getId() == this.txnId) {
            "[${this.txnId}] "
        } else {
            "[${currentTxn?.getId()}->${this.txnId}] "
        }
    }

    override fun getStatus(): TransactionStatus {
        return txnStatus
    }

    private fun setStatus(newStatus: TransactionStatus) {
        if (newStatus == this.txnStatus) {
            return
        }
        updateLastActiveTime()
        if (newStatus == TransactionStatus.PREPARING || newStatus == TransactionStatus.COMMITTING) {
            micrometerContext.createObs(
                EcosTxnSyncObsContext.BeforeCompletion(synchronizations, this)
            ).observe {
                synchronizations.forEach {
                    it.beforeCompletion()
                }
            }
        }
        logDebug { "Update status ${this.txnStatus} -> $newStatus" }
        this.txnStatus = newStatus
        if (this.txnStatus == TransactionStatus.ROLLED_BACK || this.txnStatus == TransactionStatus.COMMITTED) {
            micrometerContext.createObs(
                EcosTxnSyncObsContext.AfterCompletion(synchronizations, this)
            ).observe {
                synchronizations.forEach {
                    try {
                        it.afterCompletion(this.txnStatus)
                    } catch (e: Throwable) {
                        logError(e) { "Exception in sync.afterCompletion call" }
                    }
                }
            }
        }
        updateLastActiveTime()
    }

    override fun isIdle(): Boolean {
        return isIdle.get()
    }

    private fun updateLastActiveTime() {
        lastActiveTime.set(Instant.now())
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
        val executed: AtomicBoolean = AtomicBoolean(false)
    )

    private class ResourceData(
        val resource: TransactionResource,
        val name: String,
        var readOnlyOnPrepare: Boolean = false,
        var committed: Boolean = false,
        var wasPreparedToCommit: Boolean = false,
        var rolledBack: Boolean = false
    )
}
