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
import ru.citeck.ecos.txn.lib.resource.CommitPrepareStatus
import ru.citeck.ecos.txn.lib.resource.TransactionResource
import ru.citeck.ecos.txn.lib.transaction.TransactionImpl
import ru.citeck.ecos.txn.lib.transaction.TransactionStatus
import ru.citeck.ecos.txn.lib.transaction.TransactionSynchronization
import ru.citeck.ecos.txn.lib.transaction.TxnId
import ru.citeck.ecos.txn.lib.transaction.ctx.TxnManagerContext
import ru.citeck.ecos.txn.lib.transaction.xid.EcosXid

class TransactionImplTest {

    private lateinit var txnManager: TransactionManagerImpl

    @BeforeEach
    fun setUp() {
        val appMock = EcosWebAppApiMock("test-app")
        txnManager = TransactionManagerImpl()
        txnManager.init(appMock, EcosTxnProps())
        TxnContext.setManager(txnManager)
    }

    @AfterEach
    fun tearDown() {
        txnManager.shutdown()
    }

    private fun createTxn(readOnly: Boolean = false): TransactionImpl {
        return TransactionImpl(
            TxnId.create("test-app", "inst"),
            "test-app",
            readOnly,
            EcosMicrometerContext.NOOP
        )
    }

    private fun noopCtx(): TxnManagerContext {
        return object : TxnManagerContext {
            override fun registerAction(type: TxnActionType, actionRef: TxnActionRef) {}
            override fun registerXids(appName: String, xids: Collection<EcosXid>) {}
        }
    }

    // ==================== Status transitions ====================

    @Test
    fun `new transaction has NEW status`() {
        val txn = createTxn()
        assertThat(txn.getStatus()).isEqualTo(TransactionStatus.NEW)
    }

    @Test
    fun `start transitions from NEW to ACTIVE`() {
        val txn = createTxn()
        txn.start()
        assertThat(txn.getStatus()).isEqualTo(TransactionStatus.ACTIVE)
    }

    @Test
    fun `start on non-NEW status throws`() {
        val txn = createTxn()
        txn.start()
        assertThatThrownBy { txn.start() }
            .hasMessageContaining("Invalid status")
    }

    // ==================== isEmpty / isCompleted ====================

    @Test
    fun `isEmpty returns true when no resources and no actions`() {
        val txn = createTxn()
        txn.start()
        assertThat(txn.isEmpty()).isTrue()
    }

    @Test
    fun `isEmpty returns false after adding resource`() {
        val txn = createTxn()
        txn.start()
        txn.doWithinTxn(noopCtx(), false) {
            txn.getOrAddRes("key") { _, id -> TestRes("res", id) }
        }
        assertThat(txn.isEmpty()).isFalse()
    }

    @Test
    fun `isCompleted returns false for ACTIVE`() {
        val txn = createTxn()
        txn.start()
        assertThat(txn.isCompleted()).isFalse()
    }

    @Test
    fun `isCompleted returns true for COMMITTED`() {
        val txn = createTxn()
        txn.start()
        txn.onePhaseCommit()
        assertThat(txn.isCompleted()).isTrue()
        assertThat(txn.getStatus()).isEqualTo(TransactionStatus.COMMITTED)
    }

    @Test
    fun `isCompleted returns true for ROLLED_BACK`() {
        val txn = createTxn()
        txn.start()
        txn.rollback(null)
        assertThat(txn.isCompleted()).isTrue()
        assertThat(txn.getStatus()).isEqualTo(TransactionStatus.ROLLED_BACK)
    }

    // ==================== getOrAddRes ====================

    @Test
    fun `getOrAddRes returns existing resource for same key`() {
        val txn = createTxn()
        txn.start()
        txn.doWithinTxn(noopCtx(), false) {
            val r1 = txn.getOrAddRes("key") { _, id -> TestRes("res1", id) }
            val r2 = txn.getOrAddRes("key") { _, id -> TestRes("res2", id) }
            assertThat(r1).isSameAs(r2)
        }
    }

    @Test
    fun `getOrAddRes on non-ACTIVE status throws`() {
        val txn = createTxn()
        txn.start()
        txn.onePhaseCommit()
        txn.doWithinTxn(noopCtx(), false) {
            assertThatThrownBy {
                txn.getOrAddRes("key") { _, id -> TestRes("res", id) }
            }.hasMessageContaining("not active")
        }
    }

    @Test
    fun `getOrAddRes validates xid belongs to transaction`() {
        val txn = createTxn()
        txn.start()
        val wrongTxnId = TxnId.create("other", "inst")
        txn.doWithinTxn(noopCtx(), false) {
            assertThatThrownBy {
                txn.getOrAddRes("key") { _, _ -> TestRes("res", wrongTxnId) }
            }.hasMessageContaining("not belongs to transaction")
        }
    }

    @Test
    fun `getResources returns added resources`() {
        val txn = createTxn()
        txn.start()
        txn.doWithinTxn(noopCtx(), false) {
            txn.getOrAddRes("k1") { _, id -> TestRes("r1", id) }
            txn.getOrAddRes("k2") { _, id -> TestRes("r2", id) }
        }
        assertThat(txn.getResources()).hasSize(2)
        assertThat(txn.getResourcesNames()).containsExactly("r1", "r2")
    }

    // ==================== registerXids ====================

    @Test
    fun `registerXids on non-ACTIVE status throws`() {
        val txn = createTxn()
        txn.start()
        txn.onePhaseCommit()
        assertThatThrownBy {
            txn.registerXids("app", emptyList())
        }.hasMessageContaining("not active")
    }

    // ==================== prepareCommit ====================

    @Test
    fun `prepareCommit returns xids for prepared resources`() {
        val txn = createTxn()
        txn.start()
        txn.doWithinTxn(noopCtx(), false) {
            txn.getOrAddRes("k1") { _, id -> TestRes("r1", id) }
        }
        val xids = txn.prepareCommit()
        assertThat(xids).hasSize(1)
        assertThat(txn.getStatus()).isEqualTo(TransactionStatus.PREPARED)
    }

    @Test
    fun `prepareCommit with read-only resource returns empty xids`() {
        val txn = createTxn()
        txn.start()
        txn.doWithinTxn(noopCtx(), false) {
            txn.getOrAddRes("k1") { _, id ->
                TestRes("r1", id, prepareResult = CommitPrepareStatus.NOTHING_TO_COMMIT)
            }
        }
        val xids = txn.prepareCommit()
        assertThat(xids).isEmpty()
    }

    @Test
    fun `prepareCommit with actions but no prepared resources creates placeholder xid`() {
        val txn = createTxn()
        txn.start()
        txn.doWithinTxn(noopCtx(), false) {
            txn.getOrAddRes("k1") { _, id ->
                TestRes("r1", id, prepareResult = CommitPrepareStatus.NOTHING_TO_COMMIT)
            }
            txn.addAction(TxnActionType.AFTER_COMMIT, 0f) {}
        }
        val xids = txn.prepareCommit()
        // Placeholder xid created because there are actions
        assertThat(xids).hasSize(1)
    }

    // ==================== commitPrepared ====================

    @Test
    fun `commitPrepared skips read-only resources`() {
        val events = mutableListOf<String>()
        val txn = createTxn()
        txn.start()
        txn.doWithinTxn(noopCtx(), false) {
            txn.getOrAddRes("k1") { _, id -> TestRes("r1", id, events = events) }
            txn.getOrAddRes("k2") { _, id ->
                TestRes("r2", id, events = events, prepareResult = CommitPrepareStatus.NOTHING_TO_COMMIT)
            }
        }
        txn.prepareCommit()
        txn.commitPrepared()

        assertThat(events).contains("r1:commitPrepared")
        assertThat(events).doesNotContain("r2:commitPrepared")
        assertThat(txn.getStatus()).isEqualTo(TransactionStatus.COMMITTED)
    }

    @Test
    fun `commitPrepared on non-PREPARED status throws`() {
        val txn = createTxn()
        txn.start()
        assertThatThrownBy { txn.commitPrepared() }
            .hasMessageContaining("Invalid status")
    }

    // ==================== onePhaseCommit ====================

    @Test
    fun `onePhaseCommit with no resources commits directly`() {
        val txn = createTxn()
        txn.start()
        txn.onePhaseCommit()
        assertThat(txn.getStatus()).isEqualTo(TransactionStatus.COMMITTED)
    }

    @Test
    fun `onePhaseCommit with single resource uses one-phase path`() {
        val events = mutableListOf<String>()
        val txn = createTxn()
        txn.start()
        txn.doWithinTxn(noopCtx(), false) {
            txn.getOrAddRes("k1") { _, id -> TestRes("r1", id, events = events) }
        }
        txn.onePhaseCommit()

        assertThat(events).contains("r1:end", "r1:onePhaseCommit")
        assertThat(events).doesNotContain("r1:prepareCommit")
    }

    @Test
    fun `onePhaseCommit with multiple resources uses prepare-then-commit`() {
        val events = mutableListOf<String>()
        val txn = createTxn()
        txn.start()
        txn.doWithinTxn(noopCtx(), false) {
            txn.getOrAddRes("k1") { _, id -> TestRes("r1", id, events = events) }
            txn.getOrAddRes("k2") { _, id -> TestRes("r2", id, events = events) }
        }
        txn.onePhaseCommit()

        assertThat(events).contains("r1:end", "r2:end", "r1:prepareCommit", "r2:prepareCommit")
        assertThat(events).contains("r1:commitPrepared", "r2:commitPrepared")
        assertThat(events).doesNotContain("r1:onePhaseCommit", "r2:onePhaseCommit")
    }

    @Test
    fun `onePhaseCommit multi-resource skips read-only on commit`() {
        val events = mutableListOf<String>()
        val txn = createTxn()
        txn.start()
        txn.doWithinTxn(noopCtx(), false) {
            txn.getOrAddRes("k1") { _, id -> TestRes("r1", id, events = events) }
            txn.getOrAddRes("k2") { _, id ->
                TestRes("r2", id, events = events, prepareResult = CommitPrepareStatus.NOTHING_TO_COMMIT)
            }
        }
        txn.onePhaseCommit()

        assertThat(events).contains("r1:commitPrepared")
        assertThat(events).doesNotContain("r2:commitPrepared")
    }

    // ==================== rollback ====================

    @Test
    fun `rollback from ACTIVE`() {
        val events = mutableListOf<String>()
        val txn = createTxn()
        txn.start()
        txn.doWithinTxn(noopCtx(), false) {
            txn.getOrAddRes("k1") { _, id -> TestRes("r1", id, events = events) }
        }
        txn.rollback(null)

        assertThat(events).contains("r1:rollback")
        assertThat(txn.getStatus()).isEqualTo(TransactionStatus.ROLLED_BACK)
    }

    @Test
    fun `rollback from PREPARED`() {
        val events = mutableListOf<String>()
        val txn = createTxn()
        txn.start()
        txn.doWithinTxn(noopCtx(), false) {
            txn.getOrAddRes("k1") { _, id -> TestRes("r1", id, events = events) }
        }
        txn.prepareCommit()
        txn.rollback(null)

        assertThat(events).contains("r1:rollback")
        assertThat(txn.getStatus()).isEqualTo(TransactionStatus.ROLLED_BACK)
    }

    @Test
    fun `rollback with exception on prepared resource rethrows when no cause`() {
        val txn = createTxn()
        txn.start()
        txn.doWithinTxn(noopCtx(), false) {
            txn.getOrAddRes("k1") { _, id ->
                TestRes("r1", id, throwOnRollback = true)
            }
        }
        txn.prepareCommit()

        // rollback(null) with prepared resource that throws => rethrows
        assertThatThrownBy {
            txn.rollback(null)
        }.isInstanceOf(RuntimeException::class.java)
    }

    @Test
    fun `rollback with exception on non-prepared resource suppresses error`() {
        val txn = createTxn()
        txn.start()
        txn.doWithinTxn(noopCtx(), false) {
            txn.getOrAddRes("k1") { _, id ->
                TestRes("r1", id, throwOnRollback = true)
            }
        }
        // Not prepared, so rollback exception is just logged
        txn.rollback(null)
        assertThat(txn.getStatus()).isEqualTo(TransactionStatus.ROLLED_BACK)
    }

    @Test
    fun `rollback with cause adds suppressed for prepared resource errors`() {
        val txn = createTxn()
        txn.start()
        txn.doWithinTxn(noopCtx(), false) {
            txn.getOrAddRes("k1") { _, id ->
                TestRes("r1", id, throwOnRollback = true)
            }
        }
        txn.prepareCommit()

        val cause = RuntimeException("original cause")
        txn.rollback(cause)
        assertThat(cause.suppressed).isNotEmpty
        assertThat(txn.getStatus()).isEqualTo(TransactionStatus.ROLLED_BACK)
    }

    @Test
    fun `rollback on COMMITTED status throws`() {
        val txn = createTxn()
        txn.start()
        txn.onePhaseCommit()
        assertThatThrownBy { txn.rollback(null) }
            .hasMessageContaining("Invalid status")
    }

    // ==================== dispose ====================

    @Test
    fun `dispose empty transaction sets DISPOSED`() {
        val txn = createTxn()
        txn.start()
        txn.dispose()
        assertThat(txn.getStatus()).isEqualTo(TransactionStatus.DISPOSED)
    }

    @Test
    fun `dispose completed transaction disposes resources`() {
        val events = mutableListOf<String>()
        val txn = createTxn()
        txn.start()
        txn.doWithinTxn(noopCtx(), false) {
            txn.getOrAddRes("k1") { _, id -> TestRes("r1", id, events = events) }
        }
        txn.onePhaseCommit()
        txn.dispose()

        assertThat(events).contains("r1:dispose")
        assertThat(txn.getStatus()).isEqualTo(TransactionStatus.DISPOSED)
    }

    @Test
    fun `dispose uncompleted readOnly transaction commits via onePhaseCommit`() {
        val events = mutableListOf<String>()
        val txn = createTxn(readOnly = true)
        txn.start()
        txn.doWithinTxn(noopCtx(), true) {
            txn.getOrAddRes("k1") { _, id -> TestRes("r1", id, events = events) }
        }
        txn.dispose()

        assertThat(events).contains("r1:onePhaseCommit", "r1:dispose")
        assertThat(txn.getStatus()).isEqualTo(TransactionStatus.DISPOSED)
    }

    @Test
    fun `dispose uncompleted non-readOnly transaction triggers rollback`() {
        val events = mutableListOf<String>()
        val txn = createTxn()
        txn.start()
        txn.doWithinTxn(noopCtx(), false) {
            txn.getOrAddRes("k1") { _, id -> TestRes("r1", id, events = events) }
        }
        txn.dispose()

        assertThat(events).contains("r1:rollback", "r1:dispose")
        assertThat(txn.getStatus()).isEqualTo(TransactionStatus.DISPOSED)
    }

    // ==================== executeAction ====================

    @Test
    fun `executeAction runs action by id`() {
        val txn = createTxn()
        txn.start()
        var executed = false
        txn.doWithinTxn(noopCtx(), false) {
            txn.addAction(TxnActionType.AFTER_COMMIT, 0f) {
                executed = true
            }
        }
        txn.executeAction(0)
        assertThat(executed).isTrue()
    }

    @Test
    fun `executeAction skips already executed action`() {
        val txn = createTxn()
        txn.start()
        var count = 0
        txn.doWithinTxn(noopCtx(), false) {
            txn.addAction(TxnActionType.AFTER_COMMIT, 0f) {
                count++
            }
        }
        txn.executeAction(0)
        txn.executeAction(0) // second call is no-op
        assertThat(count).isEqualTo(1)
    }

    @Test
    fun `executeAction with unknown id throws`() {
        val txn = createTxn()
        txn.start()
        assertThatThrownBy { txn.executeAction(999) }
            .hasMessageContaining("Action is not found by id")
    }

    // ==================== doWithReadOnlyFlag ====================

    @Test
    fun `doWithinTxn non-readOnly inside readOnly throws`() {
        val txn = createTxn(readOnly = true)
        txn.start()
        assertThatThrownBy {
            txn.doWithinTxn(noopCtx(), false) {}
        }.hasMessageContaining("readOnly")
    }

    @Test
    fun `doWithinTxn readOnly inside non-readOnly works`() {
        val txn = createTxn()
        txn.start()
        var wasReadOnly = false
        txn.doWithinTxn(noopCtx(), true) {
            wasReadOnly = txn.isReadOnly()
        }
        assertThat(wasReadOnly).isTrue()
        // Restored to non-readOnly
        assertThat(txn.isReadOnly()).isFalse()
    }

    // ==================== Synchronization ====================

    @Test
    fun `synchronization fires beforeCompletion on PREPARING`() {
        val events = mutableListOf<String>()
        val txn = createTxn()
        txn.start()
        txn.doWithinTxn(noopCtx(), false) {
            txn.getOrAddRes("k1") { _, id -> TestRes("r1", id) }
        }
        txn.registerSync(object : TransactionSynchronization {
            override fun beforeCompletion() {
                events.add("beforeCompletion")
            }
            override fun afterCompletion(status: TransactionStatus) {
                events.add("afterCompletion:$status")
            }
        })
        txn.prepareCommit()
        txn.commitPrepared()

        assertThat(events).contains("beforeCompletion")
        assertThat(events).contains("afterCompletion:COMMITTED")
    }

    @Test
    fun `synchronization fires afterCompletion on ROLLED_BACK`() {
        val events = mutableListOf<String>()
        val txn = createTxn()
        txn.start()
        txn.registerSync(object : TransactionSynchronization {
            override fun afterCompletion(status: TransactionStatus) {
                events.add("afterCompletion:$status")
            }
        })
        txn.rollback(null)

        assertThat(events).contains("afterCompletion:ROLLED_BACK")
    }

    @Test
    fun `synchronization exception in afterCompletion is caught`() {
        val txn = createTxn()
        txn.start()
        txn.registerSync(object : TransactionSynchronization {
            override fun afterCompletion(status: TransactionStatus) {
                error("sync error")
            }
        })
        // Should not throw, exception in sync is caught
        txn.rollback(null)
        assertThat(txn.getStatus()).isEqualTo(TransactionStatus.ROLLED_BACK)
    }

    @Test
    fun `registerSync on non-ACTIVE status throws`() {
        val txn = createTxn()
        txn.start()
        txn.onePhaseCommit()
        assertThatThrownBy {
            txn.registerSync(object : TransactionSynchronization {})
        }.hasMessageContaining("Invalid status")
    }

    // ==================== isIdle / lastActiveTime ====================

    @Test
    fun `isIdle is false during doWithinTxn`() {
        val txn = createTxn()
        txn.start()
        assertThat(txn.isIdle()).isTrue()
        txn.doWithinTxn(noopCtx(), false) {
            assertThat(txn.isIdle()).isFalse()
        }
        assertThat(txn.isIdle()).isTrue()
    }

    @Test
    fun `getLastActiveTime is updated`() {
        val txn = createTxn()
        val before = txn.getLastActiveTime()
        txn.start()
        val after = txn.getLastActiveTime()
        assertThat(after).isAfterOrEqualTo(before)
    }

    // ==================== getData ====================

    @Test
    fun `getData returns null for unknown key`() {
        val txn = createTxn()
        txn.start()
        assertThat(txn.getData<String>("missing")).isNull()
    }

    @Test
    fun `getData with compute caches value`() {
        val txn = createTxn()
        txn.start()
        val v1 = txn.getData("key") { "computed" }
        val v2 = txn.getData("key") { "other" }
        assertThat(v1).isEqualTo("computed")
        assertThat(v2).isEqualTo("computed")
    }

    // ==================== setReadOnly ====================

    @Test
    fun `setReadOnly updates read-only flag`() {
        val txn = createTxn()
        assertThat(txn.isReadOnly()).isFalse()
        txn.setReadOnly(true)
        assertThat(txn.isReadOnly()).isTrue()
    }

    // ==================== Helper ====================

    private class TestRes(
        private val name: String,
        private val txnId: TxnId,
        private val events: MutableList<String> = mutableListOf(),
        private val prepareResult: CommitPrepareStatus = CommitPrepareStatus.PREPARED,
        private val throwOnRollback: Boolean = false
    ) : TransactionResource {
        override fun start() {
            events.add("$name:start")
        }
        override fun end() {
            events.add("$name:end")
        }
        override fun getName() = name
        override fun getXid() = EcosXid.create(txnId, name.toByteArray())
        override fun prepareCommit(): CommitPrepareStatus {
            events.add("$name:prepareCommit")
            return prepareResult
        }
        override fun commitPrepared() {
            events.add("$name:commitPrepared")
        }
        override fun onePhaseCommit() {
            events.add("$name:onePhaseCommit")
        }
        override fun rollback() {
            events.add("$name:rollback")
            if (throwOnRollback) error("rollback error for $name")
        }
        override fun dispose() {
            events.add("$name:dispose")
        }
    }
}
