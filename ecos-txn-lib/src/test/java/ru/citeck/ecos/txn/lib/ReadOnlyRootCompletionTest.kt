package ru.citeck.ecos.txn.lib

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.assertj.core.api.Assertions.catchThrowable
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import ru.citeck.ecos.test.commons.EcosWebAppApiMock
import ru.citeck.ecos.txn.lib.action.TxnActionRef
import ru.citeck.ecos.txn.lib.action.TxnActionType
import ru.citeck.ecos.txn.lib.manager.EcosTxnProps
import ru.citeck.ecos.txn.lib.manager.TransactionManagerImpl
import ru.citeck.ecos.txn.lib.manager.TransactionPolicy
import ru.citeck.ecos.txn.lib.manager.api.client.TxnManagerRemoteApiClient
import ru.citeck.ecos.txn.lib.resource.CommitPrepareStatus
import ru.citeck.ecos.txn.lib.resource.TransactionResource
import ru.citeck.ecos.txn.lib.support.RoutingRemoteApiClient
import ru.citeck.ecos.txn.lib.transaction.ManagedTransaction
import ru.citeck.ecos.txn.lib.transaction.Transaction
import ru.citeck.ecos.txn.lib.transaction.TransactionStatus
import ru.citeck.ecos.txn.lib.transaction.TxnId
import ru.citeck.ecos.txn.lib.transaction.TxnNesting
import ru.citeck.ecos.txn.lib.transaction.ctx.TxnManagerContext
import ru.citeck.ecos.txn.lib.transaction.xid.EcosXid
import ru.citeck.ecos.webapp.api.promise.Promise
import ru.citeck.ecos.webapp.api.task.EcosTasksApi
import ru.citeck.ecos.webapp.api.task.executor.EcosTaskExecutorApi
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.atomic.AtomicInteger

/**
 * A read-only root transaction (a records query in the gateway) enlists a participant branch
 * (emodel with a database connection). The participant holds its branch until the root tells it
 * to release, so the root must notify it after a returned and after a failed action alike, after
 * its own branch is disposed, without replacing its result or its error. The notification is made
 * with the root as the current transaction, so the web layer pins it to the replica that served
 * the root. A participant the root never heard from is left to the inactivity job.
 */
class ReadOnlyRootCompletionTest {

    private val releasedEvents = listOf("db:start", "db:end", "db:onePhaseCommit", "db:dispose")

    private val events = CopyOnWriteArrayList<String>()
    private val disposeCalls = CopyOnWriteArrayList<DisposeCall>()
    private val scheduledTasks = AtomicInteger()
    private var disposeFailure: Throwable? = null
    private var rejectTasks = false

    private lateinit var gateway: TransactionManagerImpl
    private lateinit var emodel: TransactionManagerImpl

    @BeforeEach
    fun setUp() {
        val registry = HashMap<String, TransactionManagerImpl>()
        gateway = TransactionManagerImpl()
        gateway.init(GatewayWebAppApi(), EcosTxnProps(), remoteClient = GatewayRemoteClient(RoutingRemoteApiClient(registry)))
        registry["gateway"] = gateway
        emodel = TransactionManagerImpl()
        emodel.init(EcosWebAppApiMock("emodel"), EcosTxnProps(), remoteClient = RoutingRemoteApiClient(registry))
        registry["emodel"] = emodel
        TxnContext.setManager(gateway)
    }

    @AfterEach
    fun tearDown() {
        gateway.shutdown()
        emodel.shutdown()
    }

    @Test
    fun `participant branch is released after a successful read-only root`() {
        val txnId = queryThroughGateway(failure = null)

        awaitReleased(txnId)
    }

    @Test
    fun `participant branch is released after a failed read-only root`() {
        val txnId = queryThroughGateway(failure = IllegalStateException("computed attribute failed"))

        awaitReleased(txnId)
    }

    @Test
    fun `participant unknown to a failed read-only root is left to the inactivity job`() {
        val txnId = queryThroughGateway(failure = IllegalStateException("response lost"), reported = false)

        assertThat(scheduledTasks).hasValue(0)
        assertThat(gateway.getStatus(txnId)).isEqualTo(TransactionStatus.NO_TRANSACTION)
        assertThat(emodel.getStatus(txnId)).isEqualTo(TransactionStatus.ACTIVE)
        assertThat(events).containsExactly("db:start")

        // what TxnManagerJob does after the branch was idle for 20 s and the root answered NO_TRANSACTION
        emodel.disposeInactive(txnId)

        assertThat(emodel.getStatus(txnId)).isEqualTo(TransactionStatus.NO_TRANSACTION)
        assertThat(events).containsExactlyElementsOf(releasedEvents)
    }

    @Test
    fun `failed read-only root keeps its error when a participant cannot be notified`() {
        disposeFailure = IllegalStateException("peer down")
        val failure = IllegalStateException("computed attribute failed")

        val txnId = queryThroughGateway(failure)

        assertThat(failure.suppressed).isEmpty()
        awaitCondition("the notification attempt") { disposeCalls.isNotEmpty() }
        assertThat(disposeCalls.map { it.app }).containsExactly("emodel")
        assertThat(emodel.getStatus(txnId)).isEqualTo(TransactionStatus.ACTIVE)
    }

    @Test
    fun `failed read-only root keeps its error when the executor rejects the notification`() {
        rejectTasks = true
        val failure = IllegalStateException("computed attribute failed")

        val txnId = queryThroughGateway(failure)

        assertThat(failure.suppressed).isEmpty()
        assertThat(scheduledTasks).hasValue(1)
        assertThat(disposeCalls).isEmpty()
        assertThat(gateway.getStatus(txnId)).isEqualTo(TransactionStatus.NO_TRANSACTION)
        assertThat(emodel.getStatus(txnId)).isEqualTo(TransactionStatus.ACTIVE)
    }

    @Test
    fun `read-only root without remote participants schedules no notification`() {
        gateway.doInTxn(TransactionPolicy.REQUIRED, true) {
            TxnContext.getTxn().registerXids("gateway", emptySet())
        }
        assertThatThrownBy {
            gateway.doInTxn(TransactionPolicy.REQUIRED, true) { throw IllegalStateException("failed alone") }
        }.hasMessage("failed alone")

        assertThat(scheduledTasks).hasValue(0)
        assertThat(disposeCalls).isEmpty()
    }

    @Test
    fun `failed local dispose of a returned read-only root is thrown after the participant is notified once`() {
        lateinit var txnId: TxnId

        val thrown = catchThrowable {
            gateway.doInTxn(TransactionPolicy.REQUIRED, true) {
                val root = TxnContext.getTxn()
                txnId = root.getId()
                callEmodel(root, failure = null, reported = true)
                leaveCommitting(root)
            }
        }

        assertThat(thrown).hasMessageContaining("Commit is pending")
        assertThat(thrown.suppressed).describedAs("second dispose attempt").hasSize(1)
        awaitBranchReleased(txnId)
        assertThat(disposeCalls.map { it.app }).containsExactly("emodel")
        assertThat(gateway.getStatus(txnId)).isEqualTo(TransactionStatus.COMMITTING)
    }

    @Test
    fun `failed local dispose of a failed read-only root is attached to its error`() {
        val failure = IllegalStateException("computed attribute failed")
        lateinit var txnId: TxnId

        assertThatThrownBy {
            gateway.doInTxn(TransactionPolicy.REQUIRED, true) {
                val root = TxnContext.getTxn()
                txnId = root.getId()
                callEmodel(root, failure = null, reported = true)
                leaveCommitting(root)
                throw failure
            }
        }.isSameAs(failure)

        assertThat(failure.suppressed).hasSize(1)
        assertThat(failure.suppressed.single()).hasMessageContaining("Commit is pending")
        awaitBranchReleased(txnId)
        assertThat(disposeCalls.map { it.app }).containsExactly("emodel")
        assertThat(gateway.getStatus(txnId)).isEqualTo(TransactionStatus.COMMITTING)
    }

    /** records/query in the gateway: a read-only root that calls emodel through the web api. */
    private fun queryThroughGateway(failure: Throwable?, reported: Boolean = true): TxnId {
        lateinit var txnId: TxnId
        val query = {
            gateway.doInTxn(TransactionPolicy.REQUIRED, true) {
                val root = TxnContext.getTxn()
                txnId = root.getId()
                callEmodel(root, failure, reported)
            }
        }
        if (failure == null) {
            query()
        } else {
            assertThatThrownBy { query() }.isSameAs(failure)
        }
        return txnId
    }

    /**
     * The participant side (EcosWebExecutorsService) registers the xids of the branch into the
     * response, and the originator side (EcosWebClient) registers them before it checks the response
     * status. A response that never arrived ([reported] = false) leaves the root unaware of the branch.
     */
    private fun callEmodel(root: Transaction, failure: Throwable?, reported: Boolean) {
        val remoteXids = HashMap<String, MutableSet<EcosXid>>()
        val ctx = object : TxnManagerContext {
            override fun registerXids(appName: String, xids: Collection<EcosXid>) {
                remoteXids.computeIfAbsent(appName) { HashSet() }.addAll(xids)
            }
            override fun registerAction(type: TxnActionType, actionRef: TxnActionRef) {}
        }
        try {
            emodel.doInExtTxn(root.getId(), ctx, TransactionPolicy.REQUIRED, true, TxnNesting.ROOT) {
                emodel.getCurrentTransaction()!!.getOrAddRes("db") { _, id -> Resource(id) }
                if (failure != null) {
                    throw failure
                }
            }
        } finally {
            if (reported) {
                remoteXids.forEach { (app, xids) -> root.registerXids(app, xids) }
            }
        }
    }

    /** a local one-phase commit that fails leaves the root COMMITTING, the only state whose dispose throws */
    private fun leaveCommitting(root: Transaction) {
        root.getOrAddRes("local") { _, id -> FailingResource(id) }
        assertThatThrownBy { (root as ManagedTransaction).onePhaseCommit() }.hasMessage("local commit failed")
    }

    private fun awaitReleased(txnId: TxnId) {
        awaitBranchReleased(txnId)
        assertThat(disposeCalls.map { it.app }).describedAs("one notification per participant").containsExactly("emodel")
        assertThat(disposeCalls.single().rootStatus).describedAs("root disposed before the notification").isEqualTo(TransactionStatus.NO_TRANSACTION)
        assertThat(disposeCalls.single().thread).describedAs("notification thread").isNotSameAs(Thread.currentThread())
        assertThat(disposeCalls.single().currentTxnId)
            .describedAs("root current in the notification: the web layer pins it to the replica that served the root")
            .isEqualTo(txnId)
    }

    private fun awaitBranchReleased(txnId: TxnId) {
        awaitCondition("release of the emodel branch") { emodel.getStatus(txnId) == TransactionStatus.NO_TRANSACTION }
        assertThat(events).containsExactlyElementsOf(releasedEvents)
    }

    private fun awaitCondition(description: String, condition: () -> Boolean) {
        val deadline = System.currentTimeMillis() + 2_000
        while (!condition() && System.currentTimeMillis() < deadline) {
            Thread.sleep(10)
        }
        check(condition()) { "$description did not happen within 2 s" }
    }

    private class DisposeCall(val app: String, val rootStatus: TransactionStatus, val currentTxnId: TxnId?, val thread: Thread)

    private inner class GatewayRemoteClient(private val delegate: TxnManagerRemoteApiClient) : TxnManagerRemoteApiClient by delegate {
        override fun disposeTxn(app: String, txnId: TxnId) {
            disposeCalls.add(
                DisposeCall(app, gateway.getStatus(txnId), gateway.getCurrentTransaction()?.getId(), Thread.currentThread())
            )
            disposeFailure?.let { throw it }
            delegate.disposeTxn(app, txnId)
        }
    }

    /** The gateway's task executor counts the scheduled notifications and can reject them like a full pool. */
    private inner class GatewayWebAppApi : EcosWebAppApiMock("gateway") {

        private val tasks = super.getTasksApi()

        override fun getTasksApi(): EcosTasksApi = object : EcosTasksApi by tasks {
            override fun getExecutor(key: String): EcosTaskExecutorApi {
                val executor = tasks.getExecutor(key)
                return object : EcosTaskExecutorApi by executor {
                    override fun <R> execute(taskId: String, task: () -> R): Promise<R> {
                        scheduledTasks.incrementAndGet()
                        if (rejectTasks) {
                            throw RejectedExecutionException("pool is full")
                        }
                        return executor.execute(taskId, task)
                    }
                }
            }
        }
    }

    private inner class Resource(private val txnId: TxnId) : TransactionResource {
        override fun start() {
            events.add("db:start")
        }
        override fun end() {
            events.add("db:end")
        }
        override fun getName() = "db"
        override fun getXid() = EcosXid.create(txnId, "db".toByteArray())
        override fun prepareCommit() = CommitPrepareStatus.NOTHING_TO_COMMIT
        override fun commitPrepared() {
            events.add("db:commitPrepared")
        }
        override fun onePhaseCommit() {
            events.add("db:onePhaseCommit")
        }
        override fun rollback() {
            events.add("db:rollback")
        }
        override fun dispose() {
            events.add("db:dispose")
        }
    }

    private class FailingResource(private val txnId: TxnId) : TransactionResource {
        override fun start() {}
        override fun end() {}
        override fun getName() = "local"
        override fun getXid() = EcosXid.create(txnId, "local".toByteArray())
        override fun prepareCommit() = CommitPrepareStatus.NOTHING_TO_COMMIT
        override fun commitPrepared() {}
        override fun onePhaseCommit() {
            throw IllegalStateException("local commit failed")
        }
        override fun rollback() {}
        override fun dispose() {}
    }
}
