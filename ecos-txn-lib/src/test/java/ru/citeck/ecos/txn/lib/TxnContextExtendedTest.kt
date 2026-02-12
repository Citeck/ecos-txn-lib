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
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

class TxnContextExtendedTest {

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

    // ==================== doBeforeCommit outside txn ====================

    @Test
    fun `doBeforeCommit without transaction executes action immediately`() {
        val result = CopyOnWriteArrayList<String>()

        txnManager.doInTxn(TransactionPolicy.NOT_SUPPORTED, null) {
            TxnContext.doBeforeCommit(0f) { result.add("executed") }
        }

        assertThat(result).containsExactly("executed")
    }

    // ==================== doAfterCommit outside txn ====================

    @Test
    fun `doAfterCommit without transaction executes action immediately`() {
        val result = CopyOnWriteArrayList<String>()

        txnManager.doInTxn(TransactionPolicy.NOT_SUPPORTED, null) {
            TxnContext.doAfterCommit(0f, false) { result.add("executed") }
        }

        assertThat(result).containsExactly("executed")
    }

    // ==================== doAfterRollback outside txn ====================

    @Test
    fun `doAfterRollback without transaction does nothing`() {
        val result = CopyOnWriteArrayList<String>()

        txnManager.doInTxn(TransactionPolicy.NOT_SUPPORTED, null) {
            TxnContext.doAfterRollback(0f, false) { result.add("executed") }
        }

        // doAfterRollback without txn is a no-op (just returns)
        assertThat(result).isEmpty()
    }

    // ==================== getTxn ====================

    @Test
    fun `getTxn without transaction throws`() {
        txnManager.doInTxn(TransactionPolicy.NOT_SUPPORTED, null) {
            assertThatThrownBy { TxnContext.getTxn() }
                .hasMessageContaining("doesn't exists")
        }
    }

    @Test
    fun `getTxn within transaction returns it`() {
        TxnContext.doInTxn {
            val txn = TxnContext.getTxn()
            assertThat(txn).isNotNull
            assertThat(txn.getId()).isNotNull
        }
    }

    // ==================== getTxnOrNull ====================

    @Test
    fun `getTxnOrNull without transaction returns null`() {
        txnManager.doInTxn(TransactionPolicy.NOT_SUPPORTED, null) {
            assertThat(TxnContext.getTxnOrNull()).isNull()
        }
    }

    @Test
    fun `getTxnOrNull within transaction returns it`() {
        TxnContext.doInTxn {
            assertThat(TxnContext.getTxnOrNull()).isNotNull
        }
    }

    // ==================== isReadOnly ====================

    @Test
    fun `isReadOnly returns false without transaction`() {
        txnManager.doInTxn(TransactionPolicy.NOT_SUPPORTED, null) {
            assertThat(TxnContext.isReadOnly()).isFalse()
        }
    }

    @Test
    fun `isReadOnly returns true in readOnly transaction`() {
        TxnContext.doInTxn(readOnly = true) {
            assertThat(TxnContext.isReadOnly()).isTrue()
        }
    }

    @Test
    fun `isReadOnly returns false in non-readOnly transaction`() {
        TxnContext.doInTxn(readOnly = false) {
            assertThat(TxnContext.isReadOnly()).isFalse()
        }
    }

    // ==================== processListAfterCommit ====================

    @Test
    fun `processListAfterCommit batches elements`() {
        val results = CopyOnWriteArrayList<List<String>>()
        val latch = CountDownLatch(1)

        TxnContext.doInTxn {
            TxnContext.processListAfterCommit("key", "a") {
                results.add(ArrayList(it))
                latch.countDown()
            }
            TxnContext.processListAfterCommit("key", "b") {
                results.add(ArrayList(it))
                latch.countDown()
            }
        }

        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue()
        assertThat(results).hasSize(1)
        assertThat(results[0]).containsExactly("a", "b")
    }

    @Test
    fun `processListAfterCommit without transaction executes immediately`() {
        val results = CopyOnWriteArrayList<List<String>>()

        txnManager.doInTxn(TransactionPolicy.NOT_SUPPORTED, null) {
            TxnContext.processListAfterCommit("key", "a") { results.add(ArrayList(it)) }
            TxnContext.processListAfterCommit("key", "b") { results.add(ArrayList(it)) }
        }

        assertThat(results).hasSize(2)
        assertThat(results[0]).containsExactly("a")
        assertThat(results[1]).containsExactly("b")
    }

    // ==================== processSetAfterCommit ====================

    @Test
    fun `processSetAfterCommit batches and deduplicates`() {
        val results = CopyOnWriteArrayList<Set<String>>()
        val latch = CountDownLatch(1)

        TxnContext.doInTxn {
            TxnContext.processSetAfterCommit("key", "a") {
                results.add(LinkedHashSet(it))
                latch.countDown()
            }
            TxnContext.processSetAfterCommit("key", "b") {
                results.add(LinkedHashSet(it))
                latch.countDown()
            }
            TxnContext.processSetAfterCommit("key", "a") {
                results.add(LinkedHashSet(it))
                latch.countDown()
            }
        }

        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue()
        assertThat(results).hasSize(1)
        assertThat(results[0]).containsExactlyInAnyOrder("a", "b")
    }

    @Test
    fun `processSetAfterCommit without transaction executes immediately`() {
        val results = CopyOnWriteArrayList<Set<String>>()

        txnManager.doInTxn(TransactionPolicy.NOT_SUPPORTED, null) {
            TxnContext.processSetAfterCommit("key", "a") { results.add(LinkedHashSet(it)) }
            TxnContext.processSetAfterCommit("key", "b") { results.add(LinkedHashSet(it)) }
        }

        assertThat(results).hasSize(2)
    }

    // ==================== doInTxn overloads ====================

    @Test
    fun `doInTxn with requiresNew creates new transaction`() {
        TxnContext.doInTxn {
            val outerTxnId = TxnContext.getTxn().getId()
            TxnContext.doInTxn(readOnly = false, requiresNew = true) {
                assertThat(TxnContext.getTxn().getId()).isNotEqualTo(outerTxnId)
            }
        }
    }

    @Test
    fun `doInNewTxn creates new transaction`() {
        TxnContext.doInTxn {
            val outerTxnId = TxnContext.getTxn().getId()
            TxnContext.doInNewTxn {
                assertThat(TxnContext.getTxn().getId()).isNotEqualTo(outerTxnId)
            }
        }
    }

    @Test
    fun `doInNewTxn readOnly creates new readOnly transaction`() {
        TxnContext.doInNewTxn(readOnly = true) {
            assertThat(TxnContext.isReadOnly()).isTrue()
        }
    }

    @Test
    fun `getManager returns set manager`() {
        assertThat(TxnContext.getManager()).isSameAs(txnManager)
    }
}
