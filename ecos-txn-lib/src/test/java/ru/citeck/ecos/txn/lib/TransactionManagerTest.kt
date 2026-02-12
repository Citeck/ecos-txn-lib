package ru.citeck.ecos.txn.lib

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import ru.citeck.ecos.test.commons.EcosWebAppApiMock
import ru.citeck.ecos.txn.lib.manager.EcosTxnProps
import ru.citeck.ecos.txn.lib.manager.TransactionManagerImpl
import ru.citeck.ecos.txn.lib.manager.TransactionPolicy
import ru.citeck.ecos.txn.lib.resource.CommitPrepareStatus
import ru.citeck.ecos.txn.lib.resource.TransactionResource
import ru.citeck.ecos.txn.lib.transaction.TransactionStatus
import ru.citeck.ecos.txn.lib.transaction.TransactionSynchronization
import ru.citeck.ecos.txn.lib.transaction.TxnId
import ru.citeck.ecos.txn.lib.transaction.xid.EcosXid
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

class TransactionManagerTest {

    private lateinit var txnManager: TransactionManagerImpl

    @BeforeEach
    fun setUp() {
        val appApiMock = EcosWebAppApiMock("test-app")
        txnManager = TransactionManagerImpl()
        txnManager.init(appApiMock, EcosTxnProps())
        TxnContext.setManager(txnManager)
    }

    @AfterEach
    fun tearDown() {
        txnManager.shutdown()
    }

    // ==================== TransactionPolicy ====================

    @Test
    fun `REQUIRED - creates new transaction when none exists`() {
        var txnId: TxnId? = null
        txnManager.doInTxn(TransactionPolicy.REQUIRED, false) {
            txnId = TxnContext.getTxn().getId()
        }
        assertThat(txnId).isNotNull
        assertThat(TxnContext.getTxnOrNull()).isNull()
    }

    @Test
    fun `REQUIRED - reuses existing transaction`() {
        TxnContext.doInTxn {
            val outerTxnId = TxnContext.getTxn().getId()
            txnManager.doInTxn(TransactionPolicy.REQUIRED, false) {
                assertThat(TxnContext.getTxn().getId()).isEqualTo(outerTxnId)
            }
        }
    }

    @Test
    fun `REQUIRES_NEW - always creates new transaction`() {
        TxnContext.doInTxn {
            val outerTxnId = TxnContext.getTxn().getId()
            txnManager.doInTxn(TransactionPolicy.REQUIRES_NEW, false) {
                assertThat(TxnContext.getTxn().getId()).isNotEqualTo(outerTxnId)
            }
            // outer transaction is restored after inner completes
            assertThat(TxnContext.getTxn().getId()).isEqualTo(outerTxnId)
        }
    }

    @Test
    fun `SUPPORTS - runs without transaction when none exists`() {
        txnManager.doInTxn(TransactionPolicy.SUPPORTS, false) {
            assertThat(TxnContext.getTxnOrNull()).isNull()
        }
    }

    @Test
    fun `SUPPORTS - uses existing transaction when present`() {
        TxnContext.doInTxn {
            val outerTxnId = TxnContext.getTxn().getId()
            txnManager.doInTxn(TransactionPolicy.SUPPORTS, false) {
                assertThat(TxnContext.getTxn().getId()).isEqualTo(outerTxnId)
            }
        }
    }

    @Test
    fun `NOT_SUPPORTED - suspends current transaction`() {
        TxnContext.doInTxn {
            val outerTxnId = TxnContext.getTxn().getId()
            txnManager.doInTxn(TransactionPolicy.NOT_SUPPORTED, null) {
                assertThat(TxnContext.getTxnOrNull()).isNull()
            }
            // outer transaction is restored
            assertThat(TxnContext.getTxn().getId()).isEqualTo(outerTxnId)
        }
    }

    // ==================== 1PC / 2PC commit flow ====================

    @Test
    fun `single resource commits via onePhaseCommit`() {
        val events = CopyOnWriteArrayList<String>()

        TxnContext.doInTxn {
            val txnId = TxnContext.getTxn().getId()
            TxnContext.getTxn().getOrAddRes("r0") { _, _ -> TestResource("r0", txnId, events) }
        }

        assertThat(events).startsWith("r0:start", "r0:end", "r0:onePhaseCommit")
        assertThat(events).contains("r0:dispose")
        assertThat(events).doesNotContain("r0:prepareCommit", "r0:commitPrepared")
    }

    @Test
    fun `multiple resources go through prepare and commit`() {
        val events = CopyOnWriteArrayList<String>()

        TxnContext.doInTxn {
            val txnId = TxnContext.getTxn().getId()
            TxnContext.getTxn().getOrAddRes("r0") { _, _ -> TestResource("r0", txnId, events) }
            TxnContext.getTxn().getOrAddRes("r1") { _, _ -> TestResource("r1", txnId, events) }
        }

        // 2PC: end all, prepare all, commit all
        assertThat(events).contains(
            "r0:start",
            "r1:start",
            "r0:end",
            "r1:end",
            "r0:prepareCommit",
            "r1:prepareCommit",
            "r0:commitPrepared",
            "r1:commitPrepared"
        )
        // prepare happens after end, commit after prepare
        assertThat(events.indexOf("r0:prepareCommit")).isGreaterThan(events.indexOf("r1:end"))
        assertThat(events.indexOf("r0:commitPrepared")).isGreaterThan(events.indexOf("r1:prepareCommit"))
        assertThat(events).doesNotContain("r0:onePhaseCommit", "r1:onePhaseCommit")
    }

    @Test
    fun `resource returning NOTHING_TO_COMMIT is not committed`() {
        val events = CopyOnWriteArrayList<String>()

        TxnContext.doInTxn {
            val txnId = TxnContext.getTxn().getId()
            TxnContext.getTxn().getOrAddRes("r0") { _, _ -> TestResource("r0", txnId, events) }
            TxnContext.getTxn().getOrAddRes("ro") { _, _ ->
                TestResource("ro", txnId, events, prepareResult = CommitPrepareStatus.NOTHING_TO_COMMIT)
            }
        }

        assertThat(events).contains("r0:commitPrepared")
        assertThat(events).doesNotContain("ro:commitPrepared")
    }

    // ==================== Rollback ====================

    @Test
    fun `exception in user action triggers rollback of all resources`() {
        val events = CopyOnWriteArrayList<String>()

        assertThatThrownBy {
            TxnContext.doInTxn {
                val txnId = TxnContext.getTxn().getId()
                TxnContext.getTxn().getOrAddRes("r0") { _, _ -> TestResource("r0", txnId, events) }
                TxnContext.getTxn().getOrAddRes("r1") { _, _ -> TestResource("r1", txnId, events) }
                error("boom")
            }
        }.hasMessage("boom")

        assertThat(events).contains("r0:rollback", "r1:rollback")
        assertThat(events).doesNotContain("r0:commitPrepared", "r1:commitPrepared", "r0:onePhaseCommit")
    }

    @Test
    fun `exception during prepareCommit triggers rollback`() {
        val events = CopyOnWriteArrayList<String>()

        assertThatThrownBy {
            TxnContext.doInTxn {
                val txnId = TxnContext.getTxn().getId()
                TxnContext.getTxn().getOrAddRes("r0") { _, _ -> TestResource("r0", txnId, events) }
                TxnContext.getTxn().getOrAddRes("r1") { _, _ ->
                    TestResource("r1", txnId, events, throwOnPrepare = true)
                }
            }
        }.hasMessageContaining("prepare failed")

        // both resources should be rolled back
        assertThat(events).contains("r0:rollback", "r1:rollback")
        assertThat(events).doesNotContain("r0:commitPrepared", "r1:commitPrepared")
    }

    // ==================== Transaction actions ====================

    @Test
    fun `BEFORE_COMMIT action executes before resource commit`() {
        val events = CopyOnWriteArrayList<String>()

        TxnContext.doInTxn {
            val txnId = TxnContext.getTxn().getId()
            TxnContext.getTxn().getOrAddRes("r0") { _, _ -> TestResource("r0", txnId, events) }
            TxnContext.doBeforeCommit(0f) { events.add("beforeCommit") }
        }

        val beforeCommitIdx = events.indexOf("beforeCommit")
        val commitIdx = events.indexOf("r0:onePhaseCommit")
        assertThat(beforeCommitIdx).isGreaterThanOrEqualTo(0)
        assertThat(commitIdx).isGreaterThan(beforeCommitIdx)
    }

    @Test
    fun `AFTER_COMMIT action executes after successful commit`() {
        val latch = CountDownLatch(1)
        val result = CopyOnWriteArrayList<String>()

        TxnContext.doInTxn {
            val txnId = TxnContext.getTxn().getId()
            TxnContext.getTxn().getOrAddRes("r0") { _, _ ->
                TestResource("r0", txnId, CopyOnWriteArrayList())
            }
            TxnContext.doAfterCommit(0f, false) {
                result.add("afterCommit")
                latch.countDown()
            }
        }

        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue
        assertThat(result).containsExactly("afterCommit")
    }

    @Test
    fun `AFTER_ROLLBACK action executes after rollback`() {
        val result = CopyOnWriteArrayList<String>()

        assertThatThrownBy {
            TxnContext.doInTxn {
                val txnId = TxnContext.getTxn().getId()
                TxnContext.getTxn().getOrAddRes("r0") { _, _ ->
                    TestResource("r0", txnId, CopyOnWriteArrayList())
                }
                TxnContext.doAfterRollback(0f, false) {
                    result.add("afterRollback")
                }
                error("boom")
            }
        }.hasMessage("boom")

        assertThat(result).containsExactly("afterRollback")
    }

    @Test
    fun `BEFORE_COMMIT action ordering is respected`() {
        val order = CopyOnWriteArrayList<String>()

        TxnContext.doInTxn {
            TxnContext.doBeforeCommit(3f) { order.add("third") }
            TxnContext.doBeforeCommit(1f) { order.add("first") }
            TxnContext.doBeforeCommit(2f) { order.add("second") }
        }

        assertThat(order).containsExactly("first", "second", "third")
    }

    // ==================== Read-only transactions ====================

    @Test
    fun `read-only transaction does not register actions`() {
        val result = CopyOnWriteArrayList<String>()

        TxnContext.doInTxn(readOnly = true) {
            assertThat(TxnContext.isReadOnly()).isTrue
            TxnContext.doBeforeCommit(0f) { result.add("beforeCommit") }
        }

        // in read-only mode, actions are stored locally but never propagated
        // to the actions container, so they never execute
        assertThat(result).isEmpty()
    }

    @Test
    fun `read-only transaction commits resources during dispose`() {
        val events = CopyOnWriteArrayList<String>()

        TxnContext.doInTxn(readOnly = true) {
            val txnId = TxnContext.getTxn().getId()
            TxnContext.getTxn().getOrAddRes("r0") { _, _ -> TestResource("r0", txnId, events) }
        }

        // read-only: resources are committed via onePhaseCommit during dispose
        assertThat(events).contains("r0:start", "r0:end", "r0:onePhaseCommit", "r0:dispose")
    }

    @Test
    fun `non-readOnly inside readOnly throws`() {
        assertThatThrownBy {
            TxnContext.doInTxn(readOnly = true) {
                TxnContext.doInTxn(readOnly = false) {
                    // should not reach here
                }
            }
        }.hasMessageContaining("readOnly")
    }

    // ==================== Nested transactions ====================

    @Test
    fun `nested REQUIRES_NEW creates independent transaction`() {
        val txnIds = CopyOnWriteArrayList<TxnId>()
        val events = CopyOnWriteArrayList<String>()

        TxnContext.doInTxn {
            val outerTxnId = TxnContext.getTxn().getId()
            txnIds.add(outerTxnId)

            TxnContext.getTxn().getOrAddRes("outer") { _, _ -> TestResource("outer", outerTxnId, events) }

            TxnContext.doInNewTxn {
                val innerTxnId = TxnContext.getTxn().getId()
                txnIds.add(innerTxnId)
                TxnContext.getTxn().getOrAddRes("inner") { _, _ -> TestResource("inner", innerTxnId, events) }
            }

            // after inner txn, outer is restored
            txnIds.add(TxnContext.getTxn().getId())
        }

        assertThat(txnIds).hasSize(3)
        assertThat(txnIds[0]).isNotEqualTo(txnIds[1])
        assertThat(txnIds[0]).isEqualTo(txnIds[2])

        // inner transaction committed before outer
        val innerCommitIdx = events.indexOf("inner:onePhaseCommit")
        val outerCommitIdx = events.indexOf("outer:onePhaseCommit")
        assertThat(innerCommitIdx).isGreaterThanOrEqualTo(0)
        assertThat(outerCommitIdx).isGreaterThan(innerCommitIdx)
    }

    @Test
    fun `inner transaction rollback does not affect outer`() {
        val events = CopyOnWriteArrayList<String>()

        TxnContext.doInTxn {
            val outerTxnId = TxnContext.getTxn().getId()
            TxnContext.getTxn().getOrAddRes("outer") { _, _ -> TestResource("outer", outerTxnId, events) }

            try {
                TxnContext.doInNewTxn {
                    val innerTxnId = TxnContext.getTxn().getId()
                    TxnContext.getTxn().getOrAddRes("inner") { _, _ -> TestResource("inner", innerTxnId, events) }
                    error("inner fails")
                }
            } catch (_: Exception) {
                // swallow inner exception, outer should still commit
            }
        }

        assertThat(events).contains("inner:rollback")
        assertThat(events).contains("outer:onePhaseCommit")
        assertThat(events).doesNotContain("outer:rollback")
    }

    // ==================== TransactionSynchronization ====================

    @Test
    fun `synchronization callbacks fire on commit`() {
        val events = CopyOnWriteArrayList<String>()

        TxnContext.doInTxn {
            val txnId = TxnContext.getTxn().getId()
            TxnContext.getTxn().getOrAddRes("r0") { _, _ -> TestResource("r0", txnId, events) }
            TxnContext.getTxn().registerSync(object : TransactionSynchronization {
                override fun beforeCompletion() {
                    events.add("sync:beforeCompletion")
                }

                override fun afterCompletion(status: TransactionStatus) {
                    events.add("sync:afterCompletion:$status")
                }
            })
        }

        assertThat(events).contains("sync:beforeCompletion")
        assertThat(events).contains("sync:afterCompletion:COMMITTED")
        // beforeCompletion fires before the actual commit
        assertThat(events.indexOf("sync:beforeCompletion"))
            .isLessThan(events.indexOf("r0:onePhaseCommit"))
    }

    @Test
    fun `synchronization afterCompletion fires on rollback`() {
        val events = CopyOnWriteArrayList<String>()

        assertThatThrownBy {
            TxnContext.doInTxn {
                val txnId = TxnContext.getTxn().getId()
                TxnContext.getTxn().getOrAddRes("r0") { _, _ -> TestResource("r0", txnId, events) }
                TxnContext.getTxn().registerSync(object : TransactionSynchronization {
                    override fun beforeCompletion() {
                        events.add("sync:beforeCompletion")
                    }

                    override fun afterCompletion(status: TransactionStatus) {
                        events.add("sync:afterCompletion:$status")
                    }
                })
                error("boom")
            }
        }

        // beforeCompletion only fires on PREPARING/COMMITTING transitions,
        // not on rollback from ACTIVE
        assertThat(events).doesNotContain("sync:beforeCompletion")
        assertThat(events).contains("sync:afterCompletion:ROLLED_BACK")
    }

    // ==================== Transaction data ====================

    @Test
    fun `transaction data is scoped to transaction`() {
        TxnContext.doInTxn {
            val txn = TxnContext.getTxn()
            assertThat(txn.getData<String>("key")).isNull()

            val value = txn.getData("key") { "computed-$it" }
            assertThat(value).isEqualTo("computed-key")

            // second call returns cached value
            val cached = txn.getData("key") { "other-$it" }
            assertThat(cached).isEqualTo("computed-key")
        }

        // new transaction has fresh data
        TxnContext.doInTxn {
            assertThat(TxnContext.getTxn().getData<String>("key")).isNull()
        }
    }

    // ==================== Collection batching helpers ====================

    @Test
    fun `processSetBeforeCommit batches and deduplicates elements`() {
        val processedSets = CopyOnWriteArrayList<Set<String>>()

        TxnContext.doInTxn {
            TxnContext.processSetBeforeCommit("key", "a") { processedSets.add(LinkedHashSet(it)) }
            TxnContext.processSetBeforeCommit("key", "b") { processedSets.add(LinkedHashSet(it)) }
            TxnContext.processSetBeforeCommit("key", "a") { processedSets.add(LinkedHashSet(it)) }
        }

        // action fires once with deduplicated set
        assertThat(processedSets).hasSize(1)
        assertThat(processedSets[0]).containsExactlyInAnyOrder("a", "b")
    }

    @Test
    fun `processListBeforeCommit batches elements preserving duplicates`() {
        val processedLists = CopyOnWriteArrayList<List<String>>()

        TxnContext.doInTxn {
            TxnContext.processListBeforeCommit("key", "a") { processedLists.add(ArrayList(it)) }
            TxnContext.processListBeforeCommit("key", "b") { processedLists.add(ArrayList(it)) }
            TxnContext.processListBeforeCommit("key", "a") { processedLists.add(ArrayList(it)) }
        }

        assertThat(processedLists).hasSize(1)
        assertThat(processedLists[0]).containsExactly("a", "b", "a")
    }

    @Test
    fun `processSetBeforeCommit without transaction executes immediately`() {
        // without a transaction, each call fires the action immediately
        val results = CopyOnWriteArrayList<Set<String>>()

        txnManager.doInTxn(TransactionPolicy.NOT_SUPPORTED, null) {
            TxnContext.processSetBeforeCommit("key", "a") { results.add(LinkedHashSet(it)) }
            TxnContext.processSetBeforeCommit("key", "b") { results.add(LinkedHashSet(it)) }
        }

        // each call executes immediately with a single-element set
        assertThat(results).hasSize(2)
        assertThat(results[0]).containsExactly("a")
        assertThat(results[1]).containsExactly("b")
    }

    // ==================== getOrAddRes reuses existing resource ====================

    @Test
    fun `getOrAddRes returns same resource for same key`() {
        TxnContext.doInTxn {
            val txnId = TxnContext.getTxn().getId()
            val events = CopyOnWriteArrayList<String>()
            val r1 = TxnContext.getTxn().getOrAddRes("key") { _, _ -> TestResource("r", txnId, events) }
            val r2 = TxnContext.getTxn().getOrAddRes("key") { _, _ -> TestResource("other", txnId, events) }
            assertThat(r1).isSameAs(r2)
            // start is called only once
            assertThat(events.filter { it == "r:start" }).hasSize(1)
        }
    }

    // ==================== Test resource implementation ====================

    private class TestResource(
        private val name: String,
        private val txnId: TxnId,
        private val events: MutableList<String>,
        private val prepareResult: CommitPrepareStatus = CommitPrepareStatus.PREPARED,
        private val throwOnPrepare: Boolean = false
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
            if (throwOnPrepare) error("prepare failed for $name")
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
        }
        override fun dispose() {
            events.add("$name:dispose")
        }
    }
}
