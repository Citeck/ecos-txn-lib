package ru.citeck.ecos.txn.lib

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import ru.citeck.ecos.micrometer.EcosMicrometerContext
import ru.citeck.ecos.test.commons.EcosWebAppApiMock
import ru.citeck.ecos.txn.lib.action.TxnActionRef
import ru.citeck.ecos.txn.lib.action.TxnActionType
import ru.citeck.ecos.txn.lib.commit.TxnCommitData
import ru.citeck.ecos.txn.lib.manager.EcosTxnProps
import ru.citeck.ecos.txn.lib.manager.TransactionManagerImpl
import ru.citeck.ecos.txn.lib.manager.api.client.ApiVersionRes
import ru.citeck.ecos.txn.lib.manager.api.client.CurrentAppClientWrapper
import ru.citeck.ecos.txn.lib.manager.api.client.TxnManagerRemoteApiClient
import ru.citeck.ecos.txn.lib.manager.recovery.RecoverableStorage
import ru.citeck.ecos.txn.lib.resource.CommitPrepareStatus
import ru.citeck.ecos.txn.lib.resource.TransactionResource
import ru.citeck.ecos.txn.lib.transaction.TransactionImpl
import ru.citeck.ecos.txn.lib.transaction.TransactionStatus
import ru.citeck.ecos.txn.lib.transaction.TxnId
import ru.citeck.ecos.txn.lib.transaction.ctx.TxnManagerContext
import ru.citeck.ecos.txn.lib.transaction.xid.EcosXid

class CurrentAppClientWrapperTest {

    private lateinit var txnManager: TransactionManagerImpl
    private lateinit var wrapper: CurrentAppClientWrapper
    private lateinit var recorder: RecordingRemoteClient
    private lateinit var appMock: EcosWebAppApiMock
    private val appName = "test-app"

    @BeforeEach
    fun setUp() {
        appMock = EcosWebAppApiMock(appName)
        txnManager = TransactionManagerImpl()
        recorder = RecordingRemoteClient()
        txnManager.init(appMock, EcosTxnProps(), remoteClient = recorder)
        TxnContext.setManager(txnManager)
        wrapper = CurrentAppClientWrapper(recorder, txnManager)
    }

    @AfterEach
    fun tearDown() {
        txnManager.shutdown()
    }

    private fun createOpenTxn(): TxnId {
        val txnId = TxnId.create(appName, appMock.getProperties().appInstanceId)
        val txn = TransactionImpl(
            txnId,
            appName,
            false,
            EcosMicrometerContext.NOOP
        )
        val ctx = object : TxnManagerContext {
            override fun registerAction(
                type: TxnActionType,
                actionRef: TxnActionRef
            ) {}
            override fun registerXids(appName: String, xids: Collection<EcosXid>) {}
        }
        txn.start()
        txnManager.transactionsById[txnId] =
            TransactionManagerImpl.TransactionInfo(txn)
        txn.doWithinTxn(ctx, false) {
            txn.getOrAddRes("res") { _, id ->
                TestRes("res", id)
            }
        }
        return txnId
    }

    // ==================== isCurrentApp dispatch ====================

    @Test
    fun `isAppAvailable returns true for current app`() {
        assertThat(wrapper.isAppAvailable(appName)).isTrue()
        assertThat(recorder.calls).noneMatch { it.startsWith("isAppAvailable:") }
    }

    @Test
    fun `isAppAvailable delegates for remote app`() {
        assertThat(wrapper.isAppAvailable("remote-app:node-7")).isFalse()
        assertThat(recorder.calls).contains("isAppAvailable:remote-app:node-7")
        assertThat(recorder.arguments.single()).containsExactly("remote-app:node-7")
    }

    @Test
    fun `isApiVersionSupported returns SUPPORTED for current app`() {
        assertThat(wrapper.isApiVersionSupported(appName, 2)).isEqualTo(ApiVersionRes.SUPPORTED)
        assertThat(recorder.calls).noneMatch { it.startsWith("isApiVersionSupported:") }
    }

    @Test
    fun `isApiVersionSupported delegates for remote app`() {
        assertThat(wrapper.isApiVersionSupported("remote-app:node-7", 3)).isEqualTo(ApiVersionRes.SUPPORTED)
        assertThat(recorder.calls).contains("isApiVersionSupported:remote-app:node-7")
        assertThat(recorder.arguments.single()).containsExactly("remote-app:node-7", 3)
    }

    // ==================== Current app operations ====================

    @Test
    fun `onePhaseCommit for current app commits locally`() {
        val txnId = createOpenTxn()
        wrapper.onePhaseCommit(appName, txnId)
        assertThat(txnManager.getStatus(txnId)).isEqualTo(TransactionStatus.COMMITTED)
        assertThat(recorder.calls).noneMatch { it.startsWith("onePhaseCommit:") }
    }

    @Test
    fun `onePhaseCommit for remote app delegates`() {
        val txnId = TxnId.create("remote", "inst")
        wrapper.onePhaseCommit("remote-app:node-7", txnId)
        assertThat(recorder.calls).contains("onePhaseCommit:remote-app:node-7")
        assertThat(recorder.arguments.single()).containsExactly("remote-app:node-7", txnId)
    }

    @Test
    fun `prepareCommit for current app prepares locally`() {
        val txnId = createOpenTxn()
        val xids = wrapper.prepareCommit(appName, txnId)
        assertThat(xids).isNotEmpty
        assertThat(recorder.calls).noneMatch { it.startsWith("prepareCommit:") }
    }

    @Test
    fun `prepareCommit for remote app delegates`() {
        val txnId = TxnId.create("remote", "inst")
        assertThat(wrapper.prepareCommit("remote-app:node-7", txnId))
            .containsExactly(EcosXid.create(txnId, "prepared".toByteArray()))
        assertThat(recorder.calls).contains("prepareCommit:remote-app:node-7")
        assertThat(recorder.arguments.single()).containsExactly("remote-app:node-7", txnId)
    }

    @Test
    fun `commitPrepared for current app commits locally`() {
        val txnId = createOpenTxn()
        txnManager.getManagedTransaction(txnId).prepareCommit()
        wrapper.commitPrepared(appName, txnId)
        assertThat(txnManager.getStatus(txnId)).isEqualTo(TransactionStatus.COMMITTED)
    }

    @Test
    fun `commitPrepared for remote app delegates`() {
        val txnId = TxnId.create("remote", "inst")
        wrapper.commitPrepared("remote-app:node-7", txnId)
        assertThat(recorder.calls).contains("commitPrepared:remote-app:node-7")
        assertThat(recorder.arguments.single()).containsExactly("remote-app:node-7", txnId)
    }

    @Test
    fun `rollback for current app rolls back locally`() {
        val txnId = createOpenTxn()
        wrapper.rollback(appName, txnId, null)
        assertThat(txnManager.getStatus(txnId)).isEqualTo(TransactionStatus.ROLLED_BACK)
    }

    @Test
    fun `rollback for remote app delegates`() {
        val txnId = TxnId.create("remote", "inst")
        val cause = IllegalStateException("original rollback cause")
        wrapper.rollback("remote-app:node-7", txnId, cause)
        assertThat(recorder.calls).contains("rollback:remote-app:node-7")
        assertThat(recorder.arguments.single()).containsExactly("remote-app:node-7", txnId, cause)
    }

    @Test
    fun `disposeTxn for current app disposes locally`() {
        val txnId = createOpenTxn()
        wrapper.disposeTxn(appName, txnId)
        assertThat(txnManager.getStatus(txnId)).isEqualTo(TransactionStatus.NO_TRANSACTION)
    }

    @Test
    fun `disposeTxn for remote app delegates`() {
        val txnId = TxnId.create("remote", "inst")
        wrapper.disposeTxn("remote-app:node-7", txnId)
        assertThat(recorder.calls).contains("disposeTxn:remote-app:node-7")
        assertThat(recorder.arguments.single()).containsExactly("remote-app:node-7", txnId)
    }

    @Test
    fun `getTxnStatus for current app returns local status`() {
        val txnId = createOpenTxn()
        assertThat(wrapper.getTxnStatus(appName, txnId)).isEqualTo(TransactionStatus.ACTIVE)
    }

    @Test
    fun `getTxnStatus for current app with unknown txn returns NO_TRANSACTION`() {
        val txnId = TxnId.create(appName, "unknown")
        assertThat(wrapper.getTxnStatus(appName, txnId)).isEqualTo(TransactionStatus.NO_TRANSACTION)
    }

    @Test
    fun `getTxnStatus for remote app delegates`() {
        val txnId = TxnId.create("remote", "inst")
        assertThat(wrapper.getTxnStatus("remote-app:node-7", txnId)).isEqualTo(TransactionStatus.PREPARED)
        assertThat(recorder.calls).contains("getTxnStatus:remote-app:node-7")
        assertThat(recorder.arguments.single()).containsExactly("remote-app:node-7", txnId)
    }

    @Test
    fun `coordinateCommit for current app throws`() {
        val txnId = TxnId.create(appName, "inst")
        assertThatThrownBy {
            wrapper.coordinateCommit(appName, txnId, TxnCommitData.EMPTY, 0)
        }.hasMessageContaining("can't be called for current app")
    }

    @Test
    fun `coordinateCommit for remote app delegates`() {
        val txnId = TxnId.create("remote", "inst")
        val data = TxnCommitData(mapOf("participant" to setOf(EcosXid.create(txnId, "branch".toByteArray()))), emptyMap())
        wrapper.coordinateCommit("remote-app:node-7", txnId, data, 4)
        assertThat(recorder.calls).contains("coordinateCommit:remote-app:node-7")
        assertThat(recorder.arguments.single()).containsExactly("remote-app:node-7", txnId, data, 4)
    }

    @Test
    fun `executeTxnAction for current app executes locally`() {
        val txnId = createOpenTxn()
        val txn = txnManager.getManagedTransaction(txnId)
        var executed = false
        txn.doWithinTxn(
            object : TxnManagerContext {
                override fun registerAction(
                    type: TxnActionType,
                    actionRef: TxnActionRef
                ) {}
                override fun registerXids(appName: String, xids: Collection<EcosXid>) {}
            },
            false
        ) {
            txn.addAction(TxnActionType.AFTER_COMMIT, 0f) {
                executed = true
            }
        }
        wrapper.executeTxnAction(appName, txnId, 0)
        assertThat(executed).isTrue()
    }

    @Test
    fun `executeTxnAction for remote app delegates`() {
        val txnId = TxnId.create("remote", "inst")
        wrapper.executeTxnAction("remote-app:node-7", txnId, 37)
        assertThat(recorder.calls).contains("executeTxnAction:remote-app:node-7")
        assertThat(recorder.arguments.single()).containsExactly("remote-app:node-7", txnId, 37)
    }

    // ==================== appRef matching ====================

    @Test
    fun `isCurrentApp matches appName colon instanceId`() {
        val appRef = appName + ":" + appMock.getProperties().appInstanceId
        assertThat(wrapper.isAppAvailable(appRef)).isTrue()
    }

    // ==================== recovery operations ====================

    @Test
    fun `recoveryCommit for current app delegates to manager`() {
        val txnId = TxnId.create(appName, "inst")
        val xid = EcosXid.create(txnId, "branch".toByteArray())
        val storage = RecordingStorage(xid)
        txnManager.getRecoveryManager().registerStorage(storage)

        wrapper.recoveryCommit(appName, txnId, setOf(xid))

        assertThat(storage.committed).containsExactly(xid)
        assertThat(storage.rolledBack).isEmpty()
        assertThat(recorder.calls).noneMatch { it.startsWith("recoveryCommit:") }
    }

    @Test
    fun `recoveryCommit for remote app delegates`() {
        val txnId = TxnId.create("remote", "inst")
        val xids = setOf(EcosXid.create(txnId, "branch".toByteArray()))
        wrapper.recoveryCommit("remote-app:node-7", txnId, xids)
        assertThat(recorder.calls).contains("recoveryCommit:remote-app:node-7")
        assertThat(recorder.arguments.single()).containsExactly("remote-app:node-7", txnId, xids)
    }

    @Test
    fun `recoveryRollback for current app delegates to manager`() {
        val txnId = TxnId.create(appName, "inst")
        val xid = EcosXid.create(txnId, "branch".toByteArray())
        val storage = RecordingStorage(xid)
        txnManager.getRecoveryManager().registerStorage(storage)

        wrapper.recoveryRollback(appName, txnId, setOf(xid))

        assertThat(storage.rolledBack).containsExactly(xid)
        assertThat(storage.committed).isEmpty()
        assertThat(recorder.calls).noneMatch { it.startsWith("recoveryRollback:") }
    }

    @Test
    fun `recoveryRollback for remote app delegates`() {
        val txnId = TxnId.create("remote", "inst")
        val xids = setOf(EcosXid.create(txnId, "branch".toByteArray()))
        wrapper.recoveryRollback("remote-app:node-7", txnId, xids)
        assertThat(recorder.calls).contains("recoveryRollback:remote-app:node-7")
        assertThat(recorder.arguments.single()).containsExactly("remote-app:node-7", txnId, xids)
    }

    // ==================== Helpers ====================

    private class TestRes(
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

    private class RecordingStorage(private val xid: EcosXid) : RecoverableStorage {
        val committed = mutableListOf<EcosXid>()
        val rolledBack = mutableListOf<EcosXid>()

        override fun getPreparedXids(): List<EcosXid> {
            return if (committed.isEmpty() && rolledBack.isEmpty()) listOf(xid) else emptyList()
        }
        override fun commitPrepared(xid: EcosXid) {
            committed.add(xid)
        }
        override fun rollbackPrepared(xid: EcosXid) {
            rolledBack.add(xid)
        }
    }

    private class RecordingRemoteClient : TxnManagerRemoteApiClient {
        val calls = mutableListOf<String>()
        val arguments = mutableListOf<List<Any?>>()

        override fun prepareCommit(app: String, txnId: TxnId): List<EcosXid> {
            calls.add("prepareCommit:$app")
            arguments.add(listOf(app, txnId))
            return listOf(EcosXid.create(txnId, "prepared".toByteArray()))
        }
        override fun commitPrepared(app: String, txnId: TxnId) {
            calls.add("commitPrepared:$app")
            arguments.add(listOf(app, txnId))
        }
        override fun onePhaseCommit(app: String, txnId: TxnId) {
            calls.add("onePhaseCommit:$app")
            arguments.add(listOf(app, txnId))
        }
        override fun rollback(app: String, txnId: TxnId, cause: Throwable?) {
            calls.add("rollback:$app")
            arguments.add(listOf(app, txnId, cause))
        }
        override fun disposeTxn(app: String, txnId: TxnId) {
            calls.add("disposeTxn:$app")
            arguments.add(listOf(app, txnId))
        }
        override fun coordinateCommit(app: String, txnId: TxnId, data: TxnCommitData, txnLevel: Int) {
            calls.add("coordinateCommit:$app")
            arguments.add(listOf(app, txnId, data, txnLevel))
        }
        override fun recoveryCommit(app: String, txnId: TxnId, xids: Set<EcosXid>) {
            calls.add("recoveryCommit:$app")
            arguments.add(listOf(app, txnId, xids))
        }
        override fun recoveryRollback(app: String, txnId: TxnId, xids: Set<EcosXid>) {
            calls.add("recoveryRollback:$app")
            arguments.add(listOf(app, txnId, xids))
        }
        override fun getTxnStatus(app: String, txnId: TxnId): TransactionStatus {
            calls.add("getTxnStatus:$app")
            arguments.add(listOf(app, txnId))
            return TransactionStatus.PREPARED
        }
        override fun executeTxnAction(app: String, txnId: TxnId, actionId: Int) {
            calls.add("executeTxnAction:$app")
            arguments.add(listOf(app, txnId, actionId))
        }
        override fun isApiVersionSupported(app: String, version: Int): ApiVersionRes {
            calls.add("isApiVersionSupported:$app")
            arguments.add(listOf(app, version))
            return ApiVersionRes.SUPPORTED
        }
        override fun isAppAvailable(app: String): Boolean {
            calls.add("isAppAvailable:$app")
            arguments.add(listOf(app))
            return false
        }
    }
}
