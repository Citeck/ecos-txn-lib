package ru.citeck.ecos.txn.lib

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import ru.citeck.ecos.test.commons.EcosWebAppApiMock
import ru.citeck.ecos.txn.lib.action.TxnActionRef
import ru.citeck.ecos.txn.lib.action.TxnActionType
import ru.citeck.ecos.txn.lib.commit.TxnCommitData
import ru.citeck.ecos.txn.lib.manager.EcosTxnProps
import ru.citeck.ecos.txn.lib.manager.TransactionManagerImpl
import ru.citeck.ecos.txn.lib.manager.TransactionPolicy
import ru.citeck.ecos.txn.lib.manager.TxnManagerJob
import ru.citeck.ecos.txn.lib.manager.api.client.ApiVersionRes
import ru.citeck.ecos.txn.lib.manager.api.client.TxnManagerRemoteApiClient
import ru.citeck.ecos.txn.lib.resource.CommitPrepareStatus
import ru.citeck.ecos.txn.lib.resource.TransactionResource
import ru.citeck.ecos.txn.lib.transaction.ManagedTransaction
import ru.citeck.ecos.txn.lib.transaction.TransactionStatus
import ru.citeck.ecos.txn.lib.transaction.TxnId
import ru.citeck.ecos.txn.lib.transaction.ctx.TxnManagerContext
import ru.citeck.ecos.txn.lib.transaction.xid.EcosXid
import java.io.IOException
import java.time.Instant
import java.util.concurrent.CopyOnWriteArrayList

/**
 * The originator delegates its commit to a coordinator which is also a participant, and the
 * coordinate request fails on the way. Remote calls run the way the web layer runs them: inside
 * doInExtTxn(txnId, REQUIRED, readOnly = false), which recreates an empty context for a missing branch.
 */
class DelegatedCommitTransportFailureTest {

    companion object {
        private const val LOST = "unexpected end of stream"
    }

    private val managers = mutableListOf<TransactionManagerImpl>()

    @AfterEach
    fun tearDown() {
        managers.forEach { it.shutdown() }
    }

    @Test
    fun `lost coordinate request rolls back the originator first and then every participant`() {
        val fx = Fixture()

        val txnId = fx.commitWithLostResponse()

        assertThat(fx.events).contains("resA:rollback", "resB:rollback").noneMatch { it.endsWith(":prepareCommit") }
        assertThat(fx.events.count { it == "resA:rollback" }).isEqualTo(1)
        assertThat(fx.events.indexOf("resA:rollback")).isLessThan(fx.events.indexOf("rollback:app-b"))
        assertThat(fx.statuses(txnId)).containsOnly(TransactionStatus.NO_TRANSACTION)
    }

    @Test
    fun `participant left by a lost rollback is released by its job and refuses a late prepare`() {
        val fx = Fixture()
        fx.lostRollbackApps = setOf("app-b")

        val txnId = fx.commitWithLostResponse()

        assertThat(fx.statuses(txnId)).containsExactly(TransactionStatus.NO_TRANSACTION, TransactionStatus.ACTIVE)
        fx.runJob(fx.coordinator, txnId)
        assertThat(fx.events).contains("resB:rollback")
        assertThat(fx.statuses(txnId)).containsOnly(TransactionStatus.NO_TRANSACTION)
        assertThatThrownBy { fx.routing.prepareCommit("app-b", txnId) }
            .hasMessageContaining("rolled back before prepare")
    }

    @Test
    fun `delayed coordinate request cannot commit after the originator aborted`() {
        val fx = Fixture()
        var delayed: TxnCommitData? = null
        fx.onCoordinate = { _, data ->
            delayed = data
            throw IOException(LOST)
        }

        val txnId = fx.commitWithLostResponse()
        assertThatThrownBy { fx.routing.coordinateCommit("app-b", txnId, delayed!!, 0) }
            .hasMessageContaining("rolled back before prepare")

        assertThat(fx.events).noneMatch { it.endsWith(":commitPrepared") }
        assertThat(fx.statuses(txnId)).containsOnly(TransactionStatus.NO_TRANSACTION)
    }

    @Test
    fun `transaction committed by the coordinator stays committed when the response is lost`() {
        val fx = Fixture()
        fx.onCoordinate = { txnId, data ->
            fx.routing.coordinateCommit("app-b", txnId, data, 0)
            throw IOException(LOST)
        }

        val txnId = fx.commitWithLostResponse()

        assertThat(fx.events).contains("resA:commitPrepared", "resB:commitPrepared").noneMatch { it.endsWith(":rollback") }
        assertThat(fx.statuses(txnId)).containsOnly(TransactionStatus.NO_TRANSACTION)
    }

    @Test
    fun `branch prepared by the coordinator before the response was lost stays with the coordinator`() {
        val fx = Fixture()
        fx.onCoordinate = { txnId, _ ->
            fx.routing.prepareCommit("app-a", txnId)
            throw IOException(LOST)
        }

        val txnId = fx.commitWithLostResponse()
        fx.runJob(fx.initiator, txnId)

        assertThat(fx.events).noneMatch { it.endsWith(":rollback") }
        assertThat(fx.initiator.getStatus(txnId)).isEqualTo(TransactionStatus.PREPARED)
    }

    @Test
    fun `rollback decided by the coordinator is not repeated by the originator`() {
        val fx = Fixture()
        fx.failingPrepare = "resB"
        fx.onCoordinate = { txnId, data -> fx.routing.coordinateCommit("app-b", txnId, data, 0) }

        assertThatThrownBy { fx.commit() }.hasMessage("resB prepare failed")

        assertThat(fx.events.count { it == "resA:rollback" }).isEqualTo(1)
        assertThat(fx.events).doesNotContain("rollback:app-b")
        assertThat(fx.statuses(fx.lastTxnId)).containsOnly(TransactionStatus.NO_TRANSACTION)
    }

    @Test
    fun `originator without its own branch releases its context and leaves participants to their jobs`() {
        val fx = Fixture()

        val txnId = fx.commitWithLostResponse(ownBranch = false, participantResources = listOf("resB1", "resB2"))

        assertThat(fx.events).noneMatch { it.endsWith(":rollback") }
        assertThat(fx.statuses(txnId)).containsExactly(TransactionStatus.NO_TRANSACTION, TransactionStatus.ACTIVE)
        fx.runJob(fx.coordinator, txnId)
        assertThat(fx.events).contains("resB1:rollback", "resB2:rollback")
        assertThat(fx.statuses(txnId)).containsOnly(TransactionStatus.NO_TRANSACTION)
    }

    private inner class Fixture {

        val events = CopyOnWriteArrayList<String>()
        val routing = WebLayerRouting(mutableMapOf())

        var onCoordinate: (TxnId, TxnCommitData) -> Unit = { _, _ -> throw IOException(LOST) }
        var lostRollbackApps = emptySet<String>()
        var failingPrepare = ""
        var lastTxnId = TxnId.EMPTY

        val initiator: TransactionManagerImpl
        val coordinator: TransactionManagerImpl

        init {
            val initiatorClient = object : TxnManagerRemoteApiClient by routing {
                override fun coordinateCommit(app: String, txnId: TxnId, data: TxnCommitData, txnLevel: Int) {
                    events.add("coordinateCommit:$app")
                    onCoordinate(txnId, data)
                }

                override fun rollback(app: String, txnId: TxnId, cause: Throwable?) {
                    events.add("rollback:$app")
                    if (app in lostRollbackApps) {
                        throw IOException(LOST)
                    }
                    routing.rollback(app, txnId, cause)
                }
            }
            initiator = createManager("app-a", initiatorClient, EcosTxnProps(commitCoordinatorApp = "app-b"))
            coordinator = createManager("app-b", routing, EcosTxnProps())
            routing.registry["app-a"] = initiator
            routing.registry["app-b"] = coordinator
            TxnContext.setManager(initiator)
        }

        fun commitWithLostResponse(
            ownBranch: Boolean = true,
            participantResources: List<String> = listOf("resB")
        ): TxnId {
            assertThatThrownBy { commit(ownBranch, participantResources) }
                .isInstanceOf(IOException::class.java)
                .hasMessage(LOST)
            return lastTxnId
        }

        fun commit(ownBranch: Boolean = true, participantResources: List<String> = listOf("resB")) {
            TxnContext.doInNewTxn {
                val txn = TxnContext.getTxn()
                lastTxnId = txn.getId()
                if (ownBranch) {
                    txn.getOrAddRes("resA") { _, id -> TestResource("resA", id) }
                }
                txn.registerXids("app-b", participantWork(txn.getId(), participantResources))
            }
        }

        /**
         * A web call to the participant: its branch reports the xids back in the response.
         */
        private fun participantWork(txnId: TxnId, resources: List<String>): Set<EcosXid> {
            val collected = LinkedHashSet<EcosXid>()
            val ctx = object : TxnManagerContext {
                override fun registerAction(type: TxnActionType, actionRef: TxnActionRef) {}
                override fun registerXids(appName: String, xids: Collection<EcosXid>) {
                    collected.addAll(xids)
                }
            }
            coordinator.doInExtTxn(txnId, ctx, TransactionPolicy.REQUIRED, false) {
                val txn = coordinator.getCurrentTransaction()!!
                resources.forEach { name -> txn.getOrAddRes(name) { _, id -> TestResource(name, id) } }
            }
            return collected
        }

        fun statuses(txnId: TxnId): List<TransactionStatus> {
            return listOf(initiator.getStatus(txnId), coordinator.getStatus(txnId))
        }

        /**
         * One pass of the inactivity job after the context has been idle for longer than the job waits.
         */
        fun runJob(manager: TransactionManagerImpl, txnId: TxnId) {
            val info = manager.transactionsById[txnId]
            if (info != null) {
                val idle = object : ManagedTransaction by info.transaction {
                    override fun getLastActiveTime(): Instant = Instant.EPOCH
                }
                manager.transactionsById[txnId] = TransactionManagerImpl.TransactionInfo(
                    idle,
                    managerCanRecoverPreparedTxn = info.managerCanRecoverPreparedTxn,
                    lastAliveTime = 0L,
                    commitDelegatedToApp = info.commitDelegatedToApp
                )
            }
            val method = TxnManagerJob::class.java.getDeclaredMethod("disposeInactiveTransactions")
            method.isAccessible = true
            method.invoke(TxnManagerJob(manager))
        }

        private inner class TestResource(private val name: String, private val txnId: TxnId) : TransactionResource {
            override fun start() {}
            override fun end() {}
            override fun getName() = name
            override fun getXid() = EcosXid.create(txnId, name.toByteArray())
            override fun prepareCommit(): CommitPrepareStatus {
                events.add("$name:prepareCommit")
                if (name == failingPrepare) {
                    error("$name prepare failed")
                }
                return CommitPrepareStatus.PREPARED
            }
            override fun commitPrepared() {
                events.add("$name:commitPrepared")
            }
            override fun onePhaseCommit() {
                events.add("$name:onePhaseCommit")
            }
            override fun rollback() {
                events.add("$name:rollback")
            }
            override fun dispose() {}
        }
    }

    private fun createManager(app: String, client: TxnManagerRemoteApiClient, props: EcosTxnProps): TransactionManagerImpl {
        val manager = TransactionManagerImpl()
        manager.init(EcosWebAppApiMock(app), props, remoteClient = client)
        managers.add(manager)
        return manager
    }

    private class WebLayerRouting(val registry: MutableMap<String, TransactionManagerImpl>) : TxnManagerRemoteApiClient {

        private val responseCtx = object : TxnManagerContext {
            override fun registerAction(type: TxnActionType, actionRef: TxnActionRef) {}
            override fun registerXids(appName: String, xids: Collection<EcosXid>) {}
        }

        private fun <T> exec(app: String, txnId: TxnId, action: (TransactionManagerImpl) -> T): T {
            val manager = resolve(app) ?: error("No manager for '$app'")
            return manager.doInExtTxn(txnId, responseCtx, TransactionPolicy.REQUIRED, false) { action(manager) }
        }

        override fun prepareCommit(app: String, txnId: TxnId) = exec(app, txnId) { it.prepareCommitFromExtManager(txnId, true) }
        override fun commitPrepared(app: String, txnId: TxnId) = exec(app, txnId) { it.getManagedTransaction(txnId).commitPrepared() }
        override fun onePhaseCommit(app: String, txnId: TxnId) = exec(app, txnId) { it.getManagedTransaction(txnId).onePhaseCommit() }
        override fun rollback(app: String, txnId: TxnId, cause: Throwable?) =
            exec(app, txnId) { it.getManagedTransaction(txnId).rollback(null) }
        override fun disposeTxn(app: String, txnId: TxnId) = exec(app, txnId) { it.dispose(txnId) }
        override fun coordinateCommit(app: String, txnId: TxnId, data: TxnCommitData, txnLevel: Int) =
            exec(app, txnId) { it.coordinateCommit(txnId, data, txnLevel) }
        override fun executeTxnAction(app: String, txnId: TxnId, actionId: Int) =
            exec(app, txnId) { it.getManagedTransaction(txnId).executeAction(actionId) }
        override fun recoveryCommit(app: String, txnId: TxnId, xids: Set<EcosXid>) = resolve(app)!!.recoveryCommit(txnId, xids)
        override fun recoveryRollback(app: String, txnId: TxnId, xids: Set<EcosXid>) =
            resolve(app)!!.recoveryRollback(txnId, xids)
        override fun getTxnStatus(app: String, txnId: TxnId) = resolve(app)!!.getStatus(txnId)
        override fun isApiVersionSupported(app: String, version: Int) =
            if (resolve(app) != null) ApiVersionRes.SUPPORTED else ApiVersionRes.APP_NOT_AVAILABLE
        override fun isAppAvailable(app: String) = resolve(app) != null

        private fun resolve(app: String): TransactionManagerImpl? {
            return registry[app] ?: registry.values.firstOrNull {
                val props = it.webAppApi.getProperties()
                app == "${props.appName}:${props.appInstanceId}"
            }
        }
    }
}
