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
import ru.citeck.ecos.txn.lib.manager.EcosTxnProps
import ru.citeck.ecos.txn.lib.manager.TransactionManagerImpl
import ru.citeck.ecos.txn.lib.manager.TransactionPolicy
import ru.citeck.ecos.txn.lib.manager.recovery.RecoverableStorage
import ru.citeck.ecos.txn.lib.resource.CommitPrepareStatus
import ru.citeck.ecos.txn.lib.resource.TransactionResource
import ru.citeck.ecos.txn.lib.transaction.TransactionImpl
import ru.citeck.ecos.txn.lib.transaction.TransactionStatus
import ru.citeck.ecos.txn.lib.transaction.TxnId
import ru.citeck.ecos.txn.lib.transaction.ctx.TxnManagerContext
import ru.citeck.ecos.txn.lib.transaction.xid.EcosXid
import java.util.concurrent.CopyOnWriteArrayList

class TransactionManagerExtendedTest {

    private lateinit var txnManager: TransactionManagerImpl
    private lateinit var appMock: EcosWebAppApiMock

    @BeforeEach
    fun setUp() {
        appMock = EcosWebAppApiMock("test-app")
        txnManager = TransactionManagerImpl()
        txnManager.init(appMock, EcosTxnProps())
        TxnContext.setManager(txnManager)
    }

    @AfterEach
    fun tearDown() {
        txnManager.shutdown()
    }

    // ==================== getStatus ====================

    @Test
    fun `getStatus returns NO_TRANSACTION for unknown txnId`() {
        val unknownId = TxnId.create("app", "inst")
        assertThat(txnManager.getStatus(unknownId)).isEqualTo(TransactionStatus.NO_TRANSACTION)
    }

    @Test
    fun `getStatus returns ACTIVE for open transaction`() {
        val txnId = createOpenTxn()
        assertThat(txnManager.getStatus(txnId)).isEqualTo(TransactionStatus.ACTIVE)
    }

    // ==================== getManagedTransaction ====================

    @Test
    fun `getManagedTransaction for unknown txnId throws`() {
        val unknownId = TxnId.create("app", "inst")
        assertThatThrownBy { txnManager.getManagedTransaction(unknownId) }
            .hasMessageContaining("not found")
    }

    @Test
    fun `getManagedTransactionOrNull for unknown txnId returns null`() {
        val unknownId = TxnId.create("app", "inst")
        assertThat(txnManager.getManagedTransactionOrNull(unknownId)).isNull()
    }

    @Test
    fun `getTransactionOrNull for unknown txnId returns null`() {
        val unknownId = TxnId.create("app", "inst")
        assertThat(txnManager.getTransactionOrNull(unknownId)).isNull()
    }

    // ==================== dispose ====================

    @Test
    fun `dispose removes transaction from map`() {
        val txnId = createOpenTxn()
        assertThat(txnManager.getStatus(txnId)).isEqualTo(TransactionStatus.ACTIVE)
        txnManager.dispose(txnId)
        assertThat(txnManager.getStatus(txnId)).isEqualTo(TransactionStatus.NO_TRANSACTION)
    }

    @Test
    fun `dispose for unknown txnId is a no-op`() {
        val unknownId = TxnId.create("app", "inst")
        txnManager.dispose(unknownId) // should not throw
    }

    // ==================== prepareCommitFromExtManager ====================

    @Test
    fun `prepareCommitFromExtManager returns xids`() {
        val txnId = createOpenTxn()
        val xids = txnManager.prepareCommitFromExtManager(txnId, true)
        assertThat(xids).isNotEmpty
    }

    @Test
    fun `prepareCommitFromExtManager with empty result disposes transaction`() {
        val txnId = createOpenTxnNoResources()
        val xids = txnManager.prepareCommitFromExtManager(txnId, false)
        assertThat(xids).isEmpty()
        assertThat(txnManager.getStatus(txnId)).isEqualTo(TransactionStatus.NO_TRANSACTION)
    }

    @Test
    fun `prepareCommitFromExtManager with unknown txnId throws`() {
        val unknownId = TxnId.create("app", "inst")
        assertThatThrownBy { txnManager.prepareCommitFromExtManager(unknownId, true) }
            .hasMessageContaining("not found")
    }

    // ==================== recoveryCommit / recoveryRollback ====================

    @Test
    fun `recoveryCommit with existing txn commits it`() {
        val txnId = createOpenTxn()
        txnManager.getManagedTransaction(txnId).prepareCommit()
        txnManager.recoveryCommit(txnId, emptyList())
        assertThat(txnManager.getStatus(txnId)).isEqualTo(TransactionStatus.COMMITTED)
    }

    @Test
    fun `recoveryCommit without existing txn delegates to recovery manager`() {
        val txnId = TxnId.create("test-app", "inst")
        val xid = EcosXid.create(txnId, "test-app", "inst")

        val storage = TestRecoverableStorage(mutableListOf(xid))
        txnManager.getRecoveryManager().registerStorage(storage)

        txnManager.recoveryCommit(txnId, listOf(xid))
        assertThat(storage.committed).containsExactly(xid)
    }

    @Test
    fun `recoveryRollback with existing txn rolls it back`() {
        val txnId = createOpenTxn()
        txnManager.recoveryRollback(txnId, emptyList())
        assertThat(txnManager.getStatus(txnId)).isEqualTo(TransactionStatus.ROLLED_BACK)
    }

    @Test
    fun `recoveryRollback without existing txn delegates to recovery manager`() {
        val txnId = TxnId.create("test-app", "inst")
        val xid = EcosXid.create(txnId, "test-app", "inst")

        val storage = TestRecoverableStorage(mutableListOf(xid))
        txnManager.getRecoveryManager().registerStorage(storage)

        txnManager.recoveryRollback(txnId, listOf(xid))
        assertThat(storage.rolledBack).containsExactly(xid)
    }

    // ==================== getCurrentTransaction ====================

    @Test
    fun `getCurrentTransaction returns null outside transaction`() {
        assertThat(txnManager.getCurrentTransaction()).isNull()
    }

    @Test
    fun `getCurrentTransaction returns current txn inside doInTxn`() {
        TxnContext.doInTxn {
            assertThat(txnManager.getCurrentTransaction()).isNotNull
        }
    }

    // ==================== Transaction policy edge cases ====================

    @Test
    fun `SUPPORTS with readOnly mismatch delegates doWithinTxn`() {
        TxnContext.doInTxn(readOnly = false) {
            // Inner SUPPORTS with readOnly=true should work within same txn
            txnManager.doInTxn(TransactionPolicy.SUPPORTS, true) {
                assertThat(TxnContext.isReadOnly()).isTrue()
            }
            // readOnly restored
            assertThat(TxnContext.isReadOnly()).isFalse()
        }
    }

    @Test
    fun `REQUIRED with readOnly mismatch delegates doWithinTxn`() {
        TxnContext.doInTxn(readOnly = false) {
            val outerTxnId = TxnContext.getTxn().getId()
            txnManager.doInTxn(TransactionPolicy.REQUIRED, true) {
                // Same transaction but readOnly flag changed
                assertThat(TxnContext.getTxn().getId()).isEqualTo(outerTxnId)
                assertThat(TxnContext.isReadOnly()).isTrue()
            }
        }
    }

    @Test
    fun `NOT_SUPPORTED suspends and restores transaction`() {
        TxnContext.doInTxn {
            val outerTxnId = TxnContext.getTxn().getId()
            txnManager.doInTxn(TransactionPolicy.NOT_SUPPORTED, null) {
                assertThat(txnManager.getCurrentTransaction()).isNull()
            }
            assertThat(TxnContext.getTxn().getId()).isEqualTo(outerTxnId)
        }
    }

    @Test
    fun `REQUIRED with null readOnly uses default`() {
        txnManager.doInTxn(TransactionPolicy.REQUIRED, null) {
            assertThat(TxnContext.isReadOnly()).isFalse()
        }
    }

    @Test
    fun `REQUIRES_NEW with null readOnly defaults to false`() {
        txnManager.doInTxn(TransactionPolicy.REQUIRES_NEW, null) {
            assertThat(TxnContext.isReadOnly()).isFalse()
        }
    }

    // ==================== Lifecycle action ordering ====================

    @Test
    fun `doBeforeCommit actions run before resource commit`() {
        val events = CopyOnWriteArrayList<String>()
        TxnContext.doInTxn {
            val txnId = TxnContext.getTxn().getId()
            TxnContext.getTxn().getOrAddRes("r0") { _, _ ->
                object : TransactionResource {
                    override fun start() {}
                    override fun end() {}
                    override fun getName() = "r0"
                    override fun getXid() = EcosXid.create(txnId, "r0".toByteArray())
                    override fun prepareCommit() = CommitPrepareStatus.PREPARED
                    override fun commitPrepared() {
                        events.add("commit")
                    }
                    override fun onePhaseCommit() {
                        events.add("commit")
                    }
                    override fun rollback() {}
                    override fun dispose() {}
                }
            }
            TxnContext.doBeforeCommit(0f) { events.add("beforeCommit") }
        }

        val beforeIdx = events.indexOf("beforeCommit")
        val commitIdx = events.indexOf("commit")
        assertThat(beforeIdx).isGreaterThanOrEqualTo(0)
        assertThat(commitIdx).isGreaterThan(beforeIdx)
    }

    // ==================== getRecoveryManager ====================

    @Test
    fun `getRecoveryManager returns non-null`() {
        assertThat(txnManager.getRecoveryManager()).isNotNull
    }

    // ==================== Helpers ====================

    private fun createOpenTxn(): TxnId {
        val txnId = TxnId.create("test-app", appMock.getProperties().appInstanceId)
        val txn = TransactionImpl(
            txnId,
            "test-app",
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

    private fun createOpenTxnNoResources(): TxnId {
        val txnId = TxnId.create("test-app", appMock.getProperties().appInstanceId)
        val txn = TransactionImpl(
            txnId,
            "test-app",
            false,
            EcosMicrometerContext.NOOP
        )
        txn.start()
        txnManager.transactionsById[txnId] =
            TransactionManagerImpl.TransactionInfo(txn)
        return txnId
    }

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

    private class TestRecoverableStorage(
        private val preparedXids: MutableList<EcosXid> = mutableListOf()
    ) : RecoverableStorage {
        val committed = mutableListOf<EcosXid>()
        val rolledBack = mutableListOf<EcosXid>()
        override fun getPreparedXids(): List<EcosXid> = preparedXids
        override fun commitPrepared(xid: EcosXid) {
            committed.add(xid)
        }
        override fun rollbackPrepared(xid: EcosXid) {
            rolledBack.add(xid)
        }
    }
}
