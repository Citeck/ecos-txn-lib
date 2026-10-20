package ru.citeck.ecos.txn.lib

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DynamicTest
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestFactory
import ru.citeck.ecos.test.commons.EcosWebAppApiMock
import ru.citeck.ecos.txn.lib.manager.EcosTxnProps
import ru.citeck.ecos.txn.lib.manager.TransactionManagerImpl
import ru.citeck.ecos.txn.lib.manager.TransactionPolicy
import ru.citeck.ecos.txn.lib.transaction.TxnId
import ru.citeck.ecos.txn.lib.transaction.ctx.NoopTxnManagerContext
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

class PolicyFailureContractTest {

    private lateinit var manager: TransactionManagerImpl

    @BeforeEach
    fun setUp() {
        manager = TransactionManagerImpl()
        manager.init(EcosWebAppApiMock("policy-contract"), EcosTxnProps())
        TxnContext.setManager(manager)
    }

    @AfterEach
    fun tearDown() {
        manager.shutdown()
    }

    @TestFactory
    fun `external policies preserve scope and restore parent after checked work failure`(): List<DynamicTest> {
        return TransactionPolicy.entries.flatMap { policy ->
            listOf(false, true).map { hasExternal ->
                DynamicTest.dynamicTest("$policy external=$hasExternal") {
                    val external = if (hasExternal) TxnId.create("origin", "instance") else TxnId.EMPTY
                    val failure = IllegalArgumentException("external work failed")
                    TxnContext.doInTxn {
                        val parent = TxnContext.getTxn()
                        assertThatThrownBy {
                            manager.doInExtTxn(external, NoopTxnManagerContext, policy, false) { work ->
                                try {
                                    val current = TxnContext.getTxnOrNull()
                                    when (policy) {
                                        TransactionPolicy.NOT_SUPPORTED -> assertThat(current).isNull()
                                        TransactionPolicy.REQUIRES_NEW -> {
                                            assertThat(current).isNotNull().isNotSameAs(parent)
                                            assertThat(current!!.getId()).isNotEqualTo(external)
                                        }
                                        TransactionPolicy.REQUIRED -> {
                                            assertThat(current).isNotNull().isNotSameAs(parent)
                                            if (hasExternal) {
                                                assertThat(current!!.getId()).isEqualTo(external)
                                            }
                                        }
                                        TransactionPolicy.SUPPORTS -> {
                                            if (hasExternal) {
                                                assertThat(current!!.getId()).isEqualTo(external)
                                            } else {
                                                assertThat(current).isSameAs(parent)
                                            }
                                        }
                                    }
                                    throw failure
                                } finally {
                                    assertThat(work.stopWork()).isFalse()
                                }
                            }
                        }.isSameAs(failure)
                        assertThat(TxnContext.getTxn()).isSameAs(parent)
                        assertThat(parent.isReadOnly()).isFalse()
                    }
                    assertThat(TxnContext.getTxnOrNull()).isNull()
                    assertThat(manager.transactionsById).isEmpty()
                }
            }
        }
    }

    @Test
    fun `after-commit failure does not change commit outcome or block following actions`() {
        val events = CopyOnWriteArrayList<String>()
        val complete = CountDownLatch(1)
        val actionFailure = IllegalStateException("after commit failed")
        TxnContext.doInTxn {
            TxnContext.doAfterCommit(0f, false) {
                events.add("failed")
                throw actionFailure
            }
            TxnContext.doAfterCommit(1f, false) {
                assertThat(TxnContext.getTxn()).isNotNull()
                events.add("continued")
                complete.countDown()
            }
            TxnContext.doAfterRollback(0f, false) { events.add("rollback") }
        }
        assertThat(complete.await(5, TimeUnit.SECONDS)).isTrue()
        assertThat(events).containsExactly("failed", "continued")
        assertThat(TxnContext.getTxnOrNull()).isNull()
    }

    @Test
    fun `after-rollback failure is attached to original cause and following rollback actions continue`() {
        val original = IllegalArgumentException("original failure")
        val callbackFailure = IllegalStateException("rollback action failed")
        val events = mutableListOf<String>()
        assertThatThrownBy {
            TxnContext.doInTxn {
                TxnContext.doAfterRollback(0f, false) { throw callbackFailure }
                TxnContext.doAfterRollback(1f, false) { events.add("continued") }
                throw original
            }
        }.isSameAs(original)
        assertThat(original.suppressed).anyMatch { it.cause === callbackFailure }
        assertThat(events).containsExactly("continued")
        assertThat(TxnContext.getTxnOrNull()).isNull()
    }
}
