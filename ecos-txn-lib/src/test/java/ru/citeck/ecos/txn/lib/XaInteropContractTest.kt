package ru.citeck.ecos.txn.lib

import jakarta.transaction.Status
import jakarta.transaction.Synchronization
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
import ru.citeck.ecos.txn.lib.resource.type.xa.JavaXaTxnManagerAdapter
import ru.citeck.ecos.txn.lib.resource.type.xa.JavaXaTxnResourceAdapter
import ru.citeck.ecos.txn.lib.transaction.TransactionImpl
import ru.citeck.ecos.txn.lib.transaction.TransactionStatus
import ru.citeck.ecos.txn.lib.transaction.TransactionSynchronization
import ru.citeck.ecos.txn.lib.transaction.TxnId
import javax.transaction.xa.XAException
import javax.transaction.xa.XAResource
import javax.transaction.xa.Xid
import jakarta.transaction.Transaction as JtaTransaction

class XaInteropContractTest {

    private lateinit var manager: TransactionManagerImpl
    private lateinit var adapter: JavaXaTxnManagerAdapter

    @BeforeEach
    fun setUp() {
        val app = EcosWebAppApiMock("xa-contract")
        manager = TransactionManagerImpl()
        manager.init(app, EcosTxnProps())
        TxnContext.setManager(manager)
        adapter = JavaXaTxnManagerAdapter(app.getProperties())
    }

    @AfterEach
    fun tearDown() {
        manager.shutdown()
    }

    @TestFactory
    fun `all ECOS states map to JTA states without a stale adapter cache`(): List<DynamicTest> {
        val states = mapOf(
            TransactionStatus.NEW to Status.STATUS_ACTIVE,
            TransactionStatus.ACTIVE to Status.STATUS_ACTIVE,
            TransactionStatus.PREPARING to Status.STATUS_PREPARING,
            TransactionStatus.PREPARED to Status.STATUS_PREPARED,
            TransactionStatus.COMMITTING to Status.STATUS_COMMITTING,
            TransactionStatus.COMMITTED to Status.STATUS_COMMITTED,
            TransactionStatus.ROLLING_BACK to Status.STATUS_ROLLING_BACK,
            TransactionStatus.ROLLED_BACK to Status.STATUS_ROLLEDBACK,
            TransactionStatus.DISPOSED to Status.STATUS_NO_TRANSACTION,
            TransactionStatus.NO_TRANSACTION to Status.STATUS_NO_TRANSACTION
        )
        return states.map { (state, expected) ->
            DynamicTest.dynamicTest("$state -> $expected") {
                TxnContext.doInTxn {
                    val txn = TxnContext.getTxn() as TransactionImpl
                    val statusField = TransactionImpl::class.java.getDeclaredField("txnStatus")
                    statusField.isAccessible = true
                    val jta = adapter.transaction!!
                    try {
                        statusField.set(txn, state)
                        assertThat(jta.status).isEqualTo(expected)
                        assertThat(adapter.status).isEqualTo(expected)
                    } finally {
                        statusField.set(txn, TransactionStatus.ACTIVE)
                    }
                }
            }
        }
    }

    @Test
    fun `transaction wrappers follow requires-new scope and restore the parent`() {
        lateinit var outer: JtaTransaction
        lateinit var inner: JtaTransaction
        TxnContext.doInTxn {
            outer = adapter.transaction!!
            assertThat(adapter.transaction).isSameAs(outer)
            TxnContext.doInTxn { assertThat(adapter.transaction).isSameAs(outer) }
            TxnContext.doInNewTxn {
                inner = adapter.transaction!!
                assertThat(inner).isNotSameAs(outer)
                assertThat(adapter.transaction).isSameAs(inner)
            }
            assertThat(adapter.transaction).isSameAs(outer)
        }
        assertThat(adapter.transaction).isNull()
        TxnContext.doInTxn {
            assertThat(adapter.transaction).isNotSameAs(outer).isNotSameAs(inner)
        }
    }

    @Test
    fun `equal distinct XA resources each start and commit while repeated identity enlists once`() {
        val first = EqualResource()
        val second = EqualResource()
        assertThat(first).isEqualTo(second)
        TxnContext.doInTxn {
            val jta = adapter.transaction!!
            assertThat(jta.enlistResource(first)).isTrue()
            assertThat(jta.enlistResource(first)).isTrue()
            assertThat(jta.enlistResource(second)).isTrue()
        }
        for (resource in listOf(first, second)) {
            assertThat(resource.calls.map { it.method }).containsExactly("start", "end", "prepare", "commit")
            assertThat(resource.calls.last().onePhase).isFalse()
            val xid = resource.calls.first().xid
            assertThat(resource.calls.map { it.xid }).allMatch { it === xid }
        }
    }

    @Test
    fun `rollback synchronization receives only rolled-back completion and original action error survives`() {
        val events = mutableListOf<String>()
        val failure = IllegalArgumentException("action failed")
        val resource = XaResourceAdapterTest.RecordingXAResource()
        assertThatThrownBy {
            TxnContext.doInTxn {
                val jta = adapter.transaction!!
                jta.enlistResource(resource)
                jta.registerSynchronization(sync(events))
                throw failure
            }
        }.isSameAs(failure)
        assertThat(events).containsExactly("after:${Status.STATUS_ROLLEDBACK}")
        assertThat(resource.calls.map { it.method }).contains("start", "rollback").doesNotContain("prepare", "commit")
        assertThat(adapter.transaction).isNull()
    }

    @Test
    fun `JTA synchronization bridge forwards callbacks and filters intermediate completion statuses`() {
        val events = mutableListOf<String>()
        TxnContext.doInTxn {
            val txn = TxnContext.getTxn() as TransactionImpl
            adapter.transaction!!.registerSynchronization(sync(events))
            val field = TransactionImpl::class.java.getDeclaredField("synchronizations")
            field.isAccessible = true
            @Suppress("UNCHECKED_CAST")
            val callbacks = field.get(txn) as MutableList<TransactionSynchronization>
            val callback = callbacks.single()
            callback.beforeCompletion()
            for (status in TransactionStatus.entries) {
                callback.afterCompletion(status)
            }
            callbacks.clear()
        }
        assertThat(events).containsExactly(
            "before",
            "after:${Status.STATUS_COMMITTED}",
            "after:${Status.STATUS_ROLLEDBACK}"
        )
    }

    @TestFactory
    fun `XA failures retain original exception and Xid at every resource operation`(): List<DynamicTest> {
        val operations: Map<String, (JavaXaTxnResourceAdapter) -> Unit> = linkedMapOf(
            "start" to { it.start() },
            "end" to { it.end() },
            "prepare" to { it.prepareCommit() },
            "commit-prepared" to { it.commitPrepared() },
            "commit-one-phase" to { it.onePhaseCommit() },
            "rollback" to { it.rollback() }
        )
        return operations.map { (name, invoke) ->
            DynamicTest.dynamicTest(name) {
                val failure = XAException(XAException.XAER_RMFAIL)
                var receivedXid: Xid? = null
                val resource = object : XAResource by XaResourceAdapterTest.RecordingXAResource() {
                    private fun fail(xid: Xid): Nothing {
                        receivedXid = xid
                        throw failure
                    }
                    override fun start(xid: Xid, flags: Int) {
                        fail(xid)
                    }
                    override fun end(xid: Xid, flags: Int) {
                        fail(xid)
                    }
                    override fun prepare(xid: Xid): Int {
                        fail(xid)
                    }
                    override fun commit(xid: Xid, onePhase: Boolean) {
                        fail(xid)
                    }
                    override fun rollback(xid: Xid) {
                        fail(xid)
                    }
                }
                val txnId = TxnId.create("xa-contract", "instance")
                val bridge = JavaXaTxnResourceAdapter(resource, "db", txnId, "xa-contract", "instance")
                assertThatThrownBy { invoke(bridge) }.isSameAs(failure)
                assertThat(receivedXid).isSameAs(bridge.getXid())
            }
        }
    }

    private fun sync(events: MutableList<String>): Synchronization {
        return object : Synchronization {
            override fun beforeCompletion() {
                events.add("before")
            }
            override fun afterCompletion(status: Int) {
                events.add("after:$status")
            }
        }
    }

    private class EqualResource : XAResource by XaResourceAdapterTest.RecordingXAResource() {
        val calls = mutableListOf<XaResourceAdapterTest.XACall>()

        override fun equals(other: Any?): Boolean {
            return other is EqualResource
        }
        override fun hashCode(): Int {
            return 1
        }
        override fun start(xid: Xid, flags: Int) {
            calls.add(XaResourceAdapterTest.XACall("start", xid, flag = flags))
        }
        override fun end(xid: Xid, flags: Int) {
            calls.add(XaResourceAdapterTest.XACall("end", xid, flag = flags))
        }
        override fun prepare(xid: Xid): Int {
            calls.add(XaResourceAdapterTest.XACall("prepare", xid))
            return XAResource.XA_OK
        }
        override fun commit(xid: Xid, onePhase: Boolean) {
            calls.add(XaResourceAdapterTest.XACall("commit", xid, onePhase = onePhase))
        }
    }
}
