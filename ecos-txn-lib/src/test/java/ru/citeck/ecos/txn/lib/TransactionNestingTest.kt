package ru.citeck.ecos.txn.lib

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import ru.citeck.ecos.test.commons.EcosWebAppApiMock
import ru.citeck.ecos.txn.lib.action.TxnActionRef
import ru.citeck.ecos.txn.lib.action.TxnActionType
import ru.citeck.ecos.txn.lib.manager.TransactionManagerImpl
import ru.citeck.ecos.txn.lib.manager.TransactionPolicy
import ru.citeck.ecos.txn.lib.transaction.TransactionStatus
import ru.citeck.ecos.txn.lib.transaction.TransactionSynchronization
import ru.citeck.ecos.txn.lib.transaction.TxnId
import ru.citeck.ecos.txn.lib.transaction.TxnNesting
import ru.citeck.ecos.txn.lib.transaction.ctx.TxnManagerContext
import ru.citeck.ecos.txn.lib.transaction.xid.EcosXid
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

class TransactionNestingTest {

    private val app = EcosWebAppApiMock("nesting-test")
    private val manager = TransactionManagerImpl().apply { init(app) }

    @AfterEach
    fun close() {
        manager.shutdown()
        app.dispose()
    }

    @Test
    fun `only independent new transactions increase depth and restore outer context`() {
        manager.doInTxn(TransactionPolicy.REQUIRES_NEW, false) {
            val root = manager.getCurrentTransaction()!!
            assertThat(TxnNesting.of(root)).isEqualTo(TxnNesting.ROOT)

            manager.doInTxn(TransactionPolicy.REQUIRED, false) {
                assertThat(manager.getCurrentTransaction()).isSameAs(root)
            }

            assertThrows<IllegalStateException> {
                manager.doInTxn(TransactionPolicy.REQUIRES_NEW, false) {
                    val child = manager.getCurrentTransaction()!!
                    assertThat(TxnNesting.of(child)).isEqualTo(TxnNesting(root.getId(), 1))
                    manager.doInTxn(TransactionPolicy.REQUIRES_NEW, false) {
                        assertThat(TxnNesting.of(manager.getCurrentTransaction())).isEqualTo(TxnNesting(child.getId(), 2))
                    }
                    throw IllegalStateException("test rollback")
                }
            }

            assertThat(manager.getCurrentTransaction()).isSameAs(root)
        }

        assertThat(manager.getCurrentTransaction()).isNull()
    }

    @Test
    fun `completion callbacks retain ancestry until the current context is restored`() {
        val actual = mutableListOf<TxnNesting>()
        val expected = mutableListOf<TxnNesting>()

        fun registerCompletionProbe() {
            val transaction = manager.getCurrentTransaction()!!
            expected.add(0, TxnNesting(transaction.getId(), TxnNesting.of(transaction).depth + 1))
            transaction.registerSync(object : TransactionSynchronization {
                override fun beforeCompletion() {
                }

                override fun afterCompletion(status: TransactionStatus) {
                    manager.doInTxn(TransactionPolicy.REQUIRES_NEW, false) {
                        actual.add(TxnNesting.of(manager.getCurrentTransaction()))
                    }
                }
            })
        }

        manager.doInTxn(TransactionPolicy.REQUIRES_NEW, false) {
            registerCompletionProbe()
            manager.doInTxn(TransactionPolicy.REQUIRES_NEW, false) {
                registerCompletionProbe()
            }
        }

        assertThat(actual).containsExactlyElementsOf(expected)
        assertThat(actual.map { it.depth }).containsExactly(2, 1)
        assertThat(manager.getCurrentTransaction()).isNull()
    }

    @Test
    fun `external participant retains ancestry across requests and read-only replacement`() {
        val parent = TxnId.create("remote", "instance")
        val childId = TxnId.create("remote", "instance")
        val nesting = TxnNesting(parent, 2)
        val context = object : TxnManagerContext {
            override fun registerXids(appName: String, xids: Collection<EcosXid>) {
            }

            override fun registerAction(type: TxnActionType, actionRef: TxnActionRef) {
            }
        }
        val worker = Executors.newSingleThreadExecutor()

        try {
            val original = manager.doInExtTxn(childId, context, TransactionPolicy.REQUIRED, true, nesting) {
                manager.getCurrentTransaction()!!.also { txn -> txn.getData("keep") { true } }
            }

            worker.submit {
                manager.doInExtTxn(childId, context, TransactionPolicy.REQUIRED, true, nesting) {
                    assertThat(manager.getCurrentTransaction()).isSameAs(original)
                    assertThat(TxnNesting.of(original)).isEqualTo(nesting)
                    manager.doInTxn(TransactionPolicy.REQUIRES_NEW, false) {
                        assertThat(TxnNesting.of(manager.getCurrentTransaction())).isEqualTo(TxnNesting(childId, 3))
                    }
                }
            }.get(5, TimeUnit.SECONDS)

            // An older sender supplies ROOT at the boundary; retained ancestry wins.
            manager.doInExtTxn(childId, context, TransactionPolicy.REQUIRED, true, TxnNesting.ROOT) {
                assertThat(manager.getCurrentTransaction()).isSameAs(original)
                assertThat(TxnNesting.of(manager.getCurrentTransaction())).isEqualTo(nesting)
            }

            manager.doInExtTxn(childId, context, TransactionPolicy.REQUIRED, false, TxnNesting.ROOT) {
                assertThat(manager.getCurrentTransaction()).isNotSameAs(original)
                assertThat(TxnNesting.of(manager.getCurrentTransaction())).isEqualTo(nesting)
                manager.doInTxn(TransactionPolicy.REQUIRES_NEW, false) {
                    assertThat(TxnNesting.of(manager.getCurrentTransaction())).isEqualTo(TxnNesting(childId, 3))
                }
            }
        } finally {
            manager.dispose(childId)
            worker.shutdownNow()
            worker.awaitTermination(5, TimeUnit.SECONDS)
        }
    }
}
