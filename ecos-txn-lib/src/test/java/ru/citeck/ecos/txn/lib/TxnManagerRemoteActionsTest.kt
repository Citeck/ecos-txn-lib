package ru.citeck.ecos.txn.lib

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import ru.citeck.ecos.commons.data.DataValue
import ru.citeck.ecos.micrometer.EcosMicrometerContext
import ru.citeck.ecos.test.commons.EcosWebAppApiMock
import ru.citeck.ecos.txn.lib.action.TxnActionRef
import ru.citeck.ecos.txn.lib.action.TxnActionType
import ru.citeck.ecos.txn.lib.commit.TxnCommitData
import ru.citeck.ecos.txn.lib.manager.EcosTxnProps
import ru.citeck.ecos.txn.lib.manager.TransactionManagerImpl
import ru.citeck.ecos.txn.lib.manager.api.client.ApiVersionRes
import ru.citeck.ecos.txn.lib.manager.api.client.TxnManagerRemoteApiClient
import ru.citeck.ecos.txn.lib.manager.api.server.TxnManagerRemoteActions
import ru.citeck.ecos.txn.lib.manager.api.server.action.TmCommitPreparedAction
import ru.citeck.ecos.txn.lib.manager.api.server.action.TmCoordinateCommitAction
import ru.citeck.ecos.txn.lib.manager.api.server.action.TmDisposeTxnAction
import ru.citeck.ecos.txn.lib.manager.api.server.action.TmExecActionAction
import ru.citeck.ecos.txn.lib.manager.api.server.action.TmGetStatusAction
import ru.citeck.ecos.txn.lib.manager.api.server.action.TmOnePhaseCommitAction
import ru.citeck.ecos.txn.lib.manager.api.server.action.TmPrepareCommitAction
import ru.citeck.ecos.txn.lib.manager.api.server.action.TmRecoveryCommitAction
import ru.citeck.ecos.txn.lib.manager.api.server.action.TmRecoveryRollbackAction
import ru.citeck.ecos.txn.lib.manager.api.server.action.TmRollbackTxnAction
import ru.citeck.ecos.txn.lib.resource.CommitPrepareStatus
import ru.citeck.ecos.txn.lib.resource.TransactionResource
import ru.citeck.ecos.txn.lib.transaction.TransactionImpl
import ru.citeck.ecos.txn.lib.transaction.TransactionStatus
import ru.citeck.ecos.txn.lib.transaction.TxnId
import ru.citeck.ecos.txn.lib.transaction.ctx.TxnManagerContext
import ru.citeck.ecos.txn.lib.transaction.xid.EcosXid

class TxnManagerRemoteActionsTest {

    private lateinit var txnManager: TransactionManagerImpl
    private lateinit var remoteActions: TxnManagerRemoteActions
    private val managers = mutableListOf<TransactionManagerImpl>()
    private var execActionFlag = false

    @BeforeEach
    fun setUp() {
        val appApiMock = EcosWebAppApiMock("test-app")
        txnManager = TransactionManagerImpl()
        txnManager.init(appApiMock, EcosTxnProps())
        TxnContext.setManager(txnManager)
        remoteActions = TxnManagerRemoteActions(txnManager)
        managers.add(txnManager)
    }

    @AfterEach
    fun tearDown() {
        managers.forEach { it.shutdown() }
    }

    private fun createOpenTxn(): TxnId {
        val txnId = TxnId.create("test-app", "test-instance")
        val txn = TransactionImpl(
            txnId,
            "test-app",
            false,
            EcosMicrometerContext.NOOP
        )
        txn.start()
        val ctx = object : TxnManagerContext {
            override fun registerAction(
                type: TxnActionType,
                actionRef: TxnActionRef
            ) {}
            override fun registerXids(appName: String, xids: Collection<EcosXid>) {}
        }
        txn.doWithinTxn(ctx, false) {
            txn.getOrAddRes("res") { _, id -> RecordingResource("res", id) }
        }
        txnManager.transactionsById[txnId] =
            TransactionManagerImpl.TransactionInfo(txn)
        return txnId
    }

    private fun createOpenTxnWithAction(): TxnId {
        val txnId = TxnId.create("test-app", "test-instance")
        val txn = TransactionImpl(
            txnId,
            "test-app",
            false,
            EcosMicrometerContext.NOOP
        )
        txn.start()
        val ctx = object : TxnManagerContext {
            override fun registerAction(
                type: TxnActionType,
                actionRef: TxnActionRef
            ) {}
            override fun registerXids(appName: String, xids: Collection<EcosXid>) {}
        }
        txn.doWithinTxn(ctx, false) {
            txn.getOrAddRes("res") { _, id -> RecordingResource("res", id) }
            txn.addAction(TxnActionType.AFTER_COMMIT, 0f) {
                execActionFlag = true
            }
        }
        txnManager.transactionsById[txnId] =
            TransactionManagerImpl.TransactionInfo(txn)
        return txnId
    }

    @Test
    fun `execute dispatches prepare-commit and returns xids for apiVer 2`() {
        val txnId = createOpenTxn()
        val data = DataValue.create(TmPrepareCommitAction.Data(txnId))
        val result = remoteActions.execute(TmPrepareCommitAction.TYPE, data, 2)
        assertThat(result).isNotNull
        val xids = result!!.get("preparedXids")
        assertThat(xids.isArray()).isTrue()
    }

    @Test
    fun `execute dispatches prepare-commit v0 returns status`() {
        val txnId = createOpenTxn()
        val data = DataValue.create(TmPrepareCommitAction.Data(txnId))
        val result = remoteActions.execute(TmPrepareCommitAction.TYPE, data, 0)
        assertThat(result).isNotNull
        assertThat(result!!.get("status").asText()).isEqualTo("PREPARED")
    }

    @Test
    fun `execute dispatches commit-prepared`() {
        val txnId = createOpenTxn()
        // First prepare, then commit
        val prepData = DataValue.create(TmPrepareCommitAction.Data(txnId))
        remoteActions.execute(TmPrepareCommitAction.TYPE, prepData, 2)

        val commitData = DataValue.create(TmCommitPreparedAction.Data(txnId))
        val result = remoteActions.execute(TmCommitPreparedAction.TYPE, commitData, 2)
        assertThat(result).isNull()
        assertThat(txnManager.getStatus(txnId)).isEqualTo(TransactionStatus.COMMITTED)
    }

    @Test
    fun `execute dispatches one-phase-commit`() {
        val txnId = createOpenTxn()
        val data = DataValue.create(TmOnePhaseCommitAction.Data(txnId))
        val result = remoteActions.execute(TmOnePhaseCommitAction.TYPE, data, 2)
        assertThat(result).isNull()
        assertThat(txnManager.getStatus(txnId)).isEqualTo(TransactionStatus.COMMITTED)
    }

    @Test
    fun `execute dispatches rollback`() {
        val txnId = createOpenTxn()
        val data = DataValue.create(TmRollbackTxnAction.Data(txnId))
        val result = remoteActions.execute(TmRollbackTxnAction.TYPE, data, 2)
        assertThat(result).isNull()
        assertThat(txnManager.getStatus(txnId)).isEqualTo(TransactionStatus.ROLLED_BACK)
    }

    @Test
    fun `execute dispatches get-status`() {
        val txnId = createOpenTxn()
        val data = DataValue.create(TmGetStatusAction.Data(txnId))
        val result = remoteActions.execute(TmGetStatusAction.TYPE, data, 2)
        assertThat(result).isNotNull
        assertThat(result!!.get("status").asText()).isEqualTo("ACTIVE")
    }

    @Test
    fun `execute dispatches dispose`() {
        val txnId = createOpenTxn()
        val data = DataValue.create(TmDisposeTxnAction.Data(txnId))
        val result = remoteActions.execute(TmDisposeTxnAction.TYPE, data, 2)
        assertThat(result).isNull()
        assertThat(txnManager.getStatus(txnId)).isEqualTo(TransactionStatus.NO_TRANSACTION)
    }

    @Test
    fun `execute dispatches exec-action`() {
        val txnId = createOpenTxnWithAction()
        val data = DataValue.create(TmExecActionAction.Data(txnId, 0))
        remoteActions.execute(TmExecActionAction.TYPE, data, 2)
        assertThat(execActionFlag).isTrue()
    }

    @Test
    fun `execute dispatches coordinate-commit with two apps`() {
        val registryMap = mutableMapOf<String, TransactionManagerImpl>()
        val routingClient = RoutingRemoteApiClient(registryMap)

        val appA = createAppSetup("app-a", routingClient)
        val appB = createAppSetup("app-b", routingClient)
        registryMap["app-a"] = appA.manager
        registryMap["app-b"] = appB.manager

        TxnContext.setManager(appA.manager)

        val txnId = TxnId.create("app-a", appA.appMock.getProperties().appInstanceId)

        val xidsA = setupAppTransaction(appA, txnId, "resA")
        val xidsB = setupAppTransaction(appB, txnId, "resB")

        val commitData = TxnCommitData(
            mapOf("app-a" to xidsA, "app-b" to xidsB),
            emptyMap()
        )

        val remoteActionsA = TxnManagerRemoteActions(appA.manager)
        val data = DataValue.create(TmCoordinateCommitAction.Data(txnId, commitData, 0))
        remoteActionsA.execute(TmCoordinateCommitAction.TYPE, data, 2)

        assertThat(appA.manager.getStatus(txnId)).isEqualTo(TransactionStatus.NO_TRANSACTION)
        assertThat(appB.manager.getStatus(txnId)).isEqualTo(TransactionStatus.NO_TRANSACTION)
    }

    @Test
    fun `execute dispatches recovery-commit and recovery-rollback`() {
        // recovery-commit with no matching txn or storage - should not throw
        val txnId = TxnId.create("test-app", "test-instance")
        val xid = EcosXid.create(txnId, "test-app", "test-instance")

        val commitData = DataValue.create(TmRecoveryCommitAction.Data(txnId, listOf(xid)))
        // No exception - recovery commit with unknown xids is a no-op
        remoteActions.execute(TmRecoveryCommitAction.TYPE, commitData, 2)

        val rollbackData = DataValue.create(TmRecoveryRollbackAction.Data(txnId, listOf(xid)))
        remoteActions.execute(TmRecoveryRollbackAction.TYPE, rollbackData, 2)
    }

    @Test
    fun `unknown action type throws`() {
        assertThatThrownBy {
            remoteActions.execute("non-existent-action", DataValue.createObj(), 2)
        }.hasMessageContaining("Unknown action type")
    }

    // ==================== Helpers ====================

    private data class AppSetup(
        val name: String,
        val manager: TransactionManagerImpl,
        val appMock: EcosWebAppApiMock
    )

    private fun createAppSetup(
        appName: String,
        routingClient: TxnManagerRemoteApiClient
    ): AppSetup {
        val appMock = EcosWebAppApiMock(appName)
        val manager = TransactionManagerImpl()
        manager.init(appMock, EcosTxnProps(), remoteClient = routingClient)
        managers.add(manager)
        return AppSetup(appName, manager, appMock)
    }

    private fun setupAppTransaction(
        app: AppSetup,
        txnId: TxnId,
        resourceName: String
    ): Set<EcosXid> {
        val collectedXids = mutableSetOf<EcosXid>()
        val txn = TransactionImpl(
            txnId,
            app.name,
            false,
            EcosMicrometerContext.NOOP
        )
        val ctx = object : TxnManagerContext {
            override fun registerAction(type: TxnActionType, actionRef: TxnActionRef) {}
            override fun registerXids(appName: String, xids: Collection<EcosXid>) {
                collectedXids.addAll(xids)
            }
        }
        txn.start()
        app.manager.transactionsById[txnId] =
            TransactionManagerImpl.TransactionInfo(txn)
        txn.doWithinTxn(ctx, false) {
            txn.getOrAddRes(resourceName) { _, id ->
                RecordingResource(resourceName, id)
            }
        }
        return collectedXids
    }

    private class RecordingResource(
        private val name: String,
        private val txnId: TxnId
    ) : TransactionResource {
        override fun start() {}
        override fun end() {}
        override fun getName() = name
        override fun getXid() = EcosXid.create(txnId, name.toByteArray())
        override fun prepareCommit() = CommitPrepareStatus.PREPARED
        override fun commitPrepared() {}
        override fun onePhaseCommit() {}
        override fun rollback() {}
        override fun dispose() {}
    }

    private class RoutingRemoteApiClient(
        private val registry: Map<String, TransactionManagerImpl>
    ) : TxnManagerRemoteApiClient {
        override fun prepareCommit(app: String, txnId: TxnId) =
            getManager(app).prepareCommitFromExtManager(txnId, true)
        override fun commitPrepared(app: String, txnId: TxnId) =
            getManager(app).getManagedTransaction(txnId).commitPrepared()
        override fun onePhaseCommit(app: String, txnId: TxnId) =
            getManager(app).getManagedTransaction(txnId).onePhaseCommit()
        override fun rollback(app: String, txnId: TxnId, cause: Throwable?) =
            getManager(app).getManagedTransaction(txnId).rollback(cause)
        override fun disposeTxn(app: String, txnId: TxnId) =
            getManager(app).dispose(txnId)
        override fun coordinateCommit(app: String, txnId: TxnId, data: TxnCommitData, txnLevel: Int) =
            getManager(app).coordinateCommit(txnId, data, txnLevel)
        override fun recoveryCommit(app: String, txnId: TxnId, xids: Set<EcosXid>) =
            getManager(app).recoveryCommit(txnId, xids)
        override fun recoveryRollback(app: String, txnId: TxnId, xids: Set<EcosXid>) =
            getManager(app).recoveryRollback(txnId, xids)
        override fun getTxnStatus(app: String, txnId: TxnId) =
            getManager(app).getStatus(txnId)
        override fun executeTxnAction(app: String, txnId: TxnId, actionId: Int) =
            getManager(app).getManagedTransaction(txnId).executeAction(actionId)
        override fun isApiVersionSupported(app: String, version: Int) =
            if (registry.containsKey(app)) ApiVersionRes.SUPPORTED else ApiVersionRes.APP_NOT_AVAILABLE
        override fun isAppAvailable(app: String) = registry.containsKey(app)
        private fun getManager(app: String) =
            registry[app] ?: registry.entries.firstOrNull { app.startsWith(it.key) }?.value
                ?: error("No manager for '$app'")
    }
}
