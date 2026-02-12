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
        assertThat(recorder.calls).doesNotContain("isAppAvailable")
    }

    @Test
    fun `isAppAvailable delegates for remote app`() {
        wrapper.isAppAvailable("remote-app")
        assertThat(recorder.calls).contains("isAppAvailable:remote-app")
    }

    @Test
    fun `isApiVersionSupported returns SUPPORTED for current app`() {
        assertThat(wrapper.isApiVersionSupported(appName, 2)).isEqualTo(ApiVersionRes.SUPPORTED)
        assertThat(recorder.calls).doesNotContain("isApiVersionSupported")
    }

    @Test
    fun `isApiVersionSupported delegates for remote app`() {
        wrapper.isApiVersionSupported("remote-app", 2)
        assertThat(recorder.calls).contains("isApiVersionSupported:remote-app")
    }

    // ==================== Current app operations ====================

    @Test
    fun `onePhaseCommit for current app commits locally`() {
        val txnId = createOpenTxn()
        wrapper.onePhaseCommit(appName, txnId)
        assertThat(txnManager.getStatus(txnId)).isEqualTo(TransactionStatus.COMMITTED)
        assertThat(recorder.calls).doesNotContain("onePhaseCommit")
    }

    @Test
    fun `onePhaseCommit for remote app delegates`() {
        val txnId = TxnId.create("remote", "inst")
        wrapper.onePhaseCommit("remote-app", txnId)
        assertThat(recorder.calls).contains("onePhaseCommit:remote-app")
    }

    @Test
    fun `prepareCommit for current app prepares locally`() {
        val txnId = createOpenTxn()
        val xids = wrapper.prepareCommit(appName, txnId)
        assertThat(xids).isNotEmpty
        assertThat(recorder.calls).doesNotContain("prepareCommit")
    }

    @Test
    fun `prepareCommit for remote app delegates`() {
        val txnId = TxnId.create("remote", "inst")
        wrapper.prepareCommit("remote-app", txnId)
        assertThat(recorder.calls).contains("prepareCommit:remote-app")
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
        wrapper.commitPrepared("remote-app", txnId)
        assertThat(recorder.calls).contains("commitPrepared:remote-app")
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
        wrapper.rollback("remote-app", txnId, null)
        assertThat(recorder.calls).contains("rollback:remote-app")
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
        wrapper.disposeTxn("remote-app", txnId)
        assertThat(recorder.calls).contains("disposeTxn:remote-app")
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
        wrapper.getTxnStatus("remote-app", txnId)
        assertThat(recorder.calls).contains("getTxnStatus:remote-app")
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
        wrapper.coordinateCommit("remote-app", txnId, TxnCommitData.EMPTY, 0)
        assertThat(recorder.calls).contains("coordinateCommit:remote-app")
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
        wrapper.executeTxnAction("remote-app", txnId, 0)
        assertThat(recorder.calls).contains("executeTxnAction:remote-app")
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
        // No matching txn, so it goes to recovery manager - no error
        wrapper.recoveryCommit(appName, txnId, emptySet())
    }

    @Test
    fun `recoveryCommit for remote app delegates`() {
        val txnId = TxnId.create("remote", "inst")
        wrapper.recoveryCommit("remote-app", txnId, emptySet())
        assertThat(recorder.calls).contains("recoveryCommit:remote-app")
    }

    @Test
    fun `recoveryRollback for current app delegates to manager`() {
        val txnId = TxnId.create(appName, "inst")
        wrapper.recoveryRollback(appName, txnId, emptySet())
    }

    @Test
    fun `recoveryRollback for remote app delegates`() {
        val txnId = TxnId.create("remote", "inst")
        wrapper.recoveryRollback("remote-app", txnId, emptySet())
        assertThat(recorder.calls).contains("recoveryRollback:remote-app")
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

    private class RecordingRemoteClient : TxnManagerRemoteApiClient {
        val calls = mutableListOf<String>()

        override fun prepareCommit(app: String, txnId: TxnId): List<EcosXid> {
            calls.add("prepareCommit:$app")
            return emptyList()
        }
        override fun commitPrepared(app: String, txnId: TxnId) {
            calls.add("commitPrepared:$app")
        }
        override fun onePhaseCommit(app: String, txnId: TxnId) {
            calls.add("onePhaseCommit:$app")
        }
        override fun rollback(app: String, txnId: TxnId, cause: Throwable?) {
            calls.add("rollback:$app")
        }
        override fun disposeTxn(app: String, txnId: TxnId) {
            calls.add("disposeTxn:$app")
        }
        override fun coordinateCommit(app: String, txnId: TxnId, data: TxnCommitData, txnLevel: Int) {
            calls.add("coordinateCommit:$app")
        }
        override fun recoveryCommit(app: String, txnId: TxnId, xids: Set<EcosXid>) {
            calls.add("recoveryCommit:$app")
        }
        override fun recoveryRollback(app: String, txnId: TxnId, xids: Set<EcosXid>) {
            calls.add("recoveryRollback:$app")
        }
        override fun getTxnStatus(app: String, txnId: TxnId): TransactionStatus {
            calls.add("getTxnStatus:$app")
            return TransactionStatus.NO_TRANSACTION
        }
        override fun executeTxnAction(app: String, txnId: TxnId, actionId: Int) {
            calls.add("executeTxnAction:$app")
        }
        override fun isApiVersionSupported(app: String, version: Int): ApiVersionRes {
            calls.add("isApiVersionSupported:$app")
            return ApiVersionRes.SUPPORTED
        }
        override fun isAppAvailable(app: String): Boolean {
            calls.add("isAppAvailable:$app")
            return false
        }
    }
}
