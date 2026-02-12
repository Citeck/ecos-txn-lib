package ru.citeck.ecos.txn.lib

import jakarta.transaction.Status
import jakarta.transaction.Synchronization
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import ru.citeck.ecos.test.commons.EcosWebAppApiMock
import ru.citeck.ecos.txn.lib.manager.EcosTxnProps
import ru.citeck.ecos.txn.lib.manager.TransactionManagerImpl
import ru.citeck.ecos.txn.lib.resource.CommitPrepareStatus
import ru.citeck.ecos.txn.lib.resource.type.xa.JavaXaTxnManagerAdapter
import ru.citeck.ecos.txn.lib.resource.type.xa.JavaXaTxnResourceAdapter
import ru.citeck.ecos.txn.lib.transaction.TxnId
import javax.transaction.xa.XAResource
import javax.transaction.xa.Xid

class XaResourceAdapterTest {

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

    // ==================== JavaXaTxnResourceAdapter tests ====================

    @Test
    fun `start delegates to XAResource with TMNOFLAGS`() {
        val xaResource = RecordingXAResource()
        val txnId = TxnId.create("test-app", "inst")
        val adapter = JavaXaTxnResourceAdapter(xaResource, "xa-res", txnId, "test-app", "inst")

        adapter.start()

        assertThat(xaResource.calls).hasSize(1)
        assertThat(xaResource.calls[0].method).isEqualTo("start")
        assertThat(xaResource.calls[0].flag).isEqualTo(XAResource.TMNOFLAGS)
    }

    @Test
    fun `end delegates with TMSUCCESS`() {
        val xaResource = RecordingXAResource()
        val txnId = TxnId.create("test-app", "inst")
        val adapter = JavaXaTxnResourceAdapter(xaResource, "xa-res", txnId, "test-app", "inst")

        adapter.end()

        assertThat(xaResource.calls).hasSize(1)
        assertThat(xaResource.calls[0].method).isEqualTo("end")
        assertThat(xaResource.calls[0].flag).isEqualTo(XAResource.TMSUCCESS)
    }

    @Test
    fun `prepareCommit returns PREPARED for XA_OK`() {
        val xaResource = RecordingXAResource(prepareResult = XAResource.XA_OK)
        val txnId = TxnId.create("test-app", "inst")
        val adapter = JavaXaTxnResourceAdapter(xaResource, "xa-res", txnId, "test-app", "inst")

        val result = adapter.prepareCommit()

        assertThat(result).isEqualTo(CommitPrepareStatus.PREPARED)
        assertThat(xaResource.calls.last().method).isEqualTo("prepare")
    }

    @Test
    fun `prepareCommit returns NOTHING_TO_COMMIT for XA_RDONLY`() {
        val xaResource = RecordingXAResource(prepareResult = XAResource.XA_RDONLY)
        val txnId = TxnId.create("test-app", "inst")
        val adapter = JavaXaTxnResourceAdapter(xaResource, "xa-res", txnId, "test-app", "inst")

        val result = adapter.prepareCommit()

        assertThat(result).isEqualTo(CommitPrepareStatus.NOTHING_TO_COMMIT)
    }

    @Test
    fun `commitPrepared calls commit with onePhase false`() {
        val xaResource = RecordingXAResource()
        val txnId = TxnId.create("test-app", "inst")
        val adapter = JavaXaTxnResourceAdapter(xaResource, "xa-res", txnId, "test-app", "inst")

        adapter.commitPrepared()

        assertThat(xaResource.calls.last().method).isEqualTo("commit")
        assertThat(xaResource.calls.last().onePhase).isFalse()
    }

    @Test
    fun `onePhaseCommit calls commit with onePhase true`() {
        val xaResource = RecordingXAResource()
        val txnId = TxnId.create("test-app", "inst")
        val adapter = JavaXaTxnResourceAdapter(xaResource, "xa-res", txnId, "test-app", "inst")

        adapter.onePhaseCommit()

        assertThat(xaResource.calls.last().method).isEqualTo("commit")
        assertThat(xaResource.calls.last().onePhase).isTrue()
    }

    @Test
    fun `rollback delegates to XAResource`() {
        val xaResource = RecordingXAResource()
        val txnId = TxnId.create("test-app", "inst")
        val adapter = JavaXaTxnResourceAdapter(xaResource, "xa-res", txnId, "test-app", "inst")

        adapter.rollback()

        assertThat(xaResource.calls.last().method).isEqualTo("rollback")
    }

    @Test
    fun `getXid returns consistent EcosXid`() {
        val xaResource = RecordingXAResource()
        val txnId = TxnId.create("test-app", "inst")
        val adapter = JavaXaTxnResourceAdapter(xaResource, "xa-res", txnId, "test-app", "inst")

        val xid1 = adapter.getXid()
        val xid2 = adapter.getXid()

        assertThat(xid1).isSameAs(xid2)
        assertThat(xid1.formatId).isEqualTo(90)
        assertThat(xid1.getTransactionId()).isEqualTo(txnId)
    }

    // ==================== JavaXaTxnManagerAdapter tests ====================

    @Test
    fun `getStatus returns NO_TRANSACTION when no txn`() {
        val appMock = EcosWebAppApiMock("test-app")
        val adapter = JavaXaTxnManagerAdapter(appMock.getProperties())

        assertThat(adapter.status).isEqualTo(Status.STATUS_NO_TRANSACTION)
    }

    @Test
    fun `getStatus maps ACTIVE correctly`() {
        val appMock = EcosWebAppApiMock("test-app")
        val adapter = JavaXaTxnManagerAdapter(appMock.getProperties())

        TxnContext.doInTxn {
            assertThat(adapter.status).isEqualTo(Status.STATUS_ACTIVE)
        }
    }

    @Test
    fun `getTransaction returns null when no txn`() {
        val appMock = EcosWebAppApiMock("test-app")
        val adapter = JavaXaTxnManagerAdapter(appMock.getProperties())

        assertThat(adapter.transaction).isNull()
    }

    @Test
    fun `getTransaction wraps ECOS transaction`() {
        val appMock = EcosWebAppApiMock("test-app")
        val adapter = JavaXaTxnManagerAdapter(appMock.getProperties())

        TxnContext.doInTxn {
            val jakartaTxn = adapter.transaction
            assertThat(jakartaTxn).isNotNull
            assertThat(jakartaTxn!!.status).isEqualTo(Status.STATUS_ACTIVE)
        }
    }

    @Test
    fun `enlistResource creates adapter and returns true`() {
        val appMock = EcosWebAppApiMock("test-app")
        val adapter = JavaXaTxnManagerAdapter(appMock.getProperties())

        TxnContext.doInTxn {
            val jakartaTxn = adapter.transaction!!
            val xaResource = RecordingXAResource()
            val result = jakartaTxn.enlistResource(xaResource)
            assertThat(result).isTrue()
        }
    }

    @Test
    fun `enlistResource same instance returns same adapter via IdentityKey`() {
        val appMock = EcosWebAppApiMock("test-app")
        val adapter = JavaXaTxnManagerAdapter(appMock.getProperties())

        TxnContext.doInTxn {
            val jakartaTxn = adapter.transaction!!
            val xaResource = RecordingXAResource()

            // Enlist same XAResource twice
            jakartaTxn.enlistResource(xaResource)
            jakartaTxn.enlistResource(xaResource)

            // start should only be called once since the second enlistResource
            // reuses the existing adapter
            assertThat(xaResource.calls.count { it.method == "start" }).isEqualTo(1)
        }
    }

    @Test
    fun `registerSynchronization bridges to ECOS sync`() {
        val appMock = EcosWebAppApiMock("test-app")
        val adapter = JavaXaTxnManagerAdapter(appMock.getProperties())

        val events = mutableListOf<String>()

        TxnContext.doInTxn {
            // Enlist a resource so the commit phase fires synchronization callbacks
            val jakartaTxn = adapter.transaction!!
            jakartaTxn.enlistResource(RecordingXAResource())
            jakartaTxn.registerSynchronization(object : Synchronization {
                override fun beforeCompletion() {
                    events.add("beforeCompletion")
                }
                override fun afterCompletion(status: Int) {
                    events.add("afterCompletion:$status")
                }
            })
        }

        assertThat(events).contains("beforeCompletion")
        assertThat(events).contains("afterCompletion:${Status.STATUS_COMMITTED}")
    }

    @Test
    fun `unsupported methods throw`() {
        val appMock = EcosWebAppApiMock("test-app")
        val adapter = JavaXaTxnManagerAdapter(appMock.getProperties())

        assertThatThrownBy { adapter.begin() }.hasMessageContaining("not supported")
        assertThatThrownBy { adapter.commit() }.hasMessageContaining("not supported")
        assertThatThrownBy { adapter.rollback() }.hasMessageContaining("not supported")
        assertThatThrownBy { adapter.resume(null) }.hasMessageContaining("not supported")
        assertThatThrownBy { adapter.setRollbackOnly() }.hasMessageContaining("not supported")
        assertThatThrownBy { adapter.suspend() }.hasMessageContaining("not supported")
    }

    // ==================== Recording XAResource ====================

    data class XACall(
        val method: String,
        val xid: Xid? = null,
        val flag: Int = -1,
        val onePhase: Boolean = false
    )

    class RecordingXAResource(
        private val prepareResult: Int = XAResource.XA_OK
    ) : XAResource {
        val calls = mutableListOf<XACall>()

        override fun start(xid: Xid, flags: Int) {
            calls.add(XACall("start", xid, flag = flags))
        }
        override fun end(xid: Xid, flags: Int) {
            calls.add(XACall("end", xid, flag = flags))
        }
        override fun prepare(xid: Xid): Int {
            calls.add(XACall("prepare", xid))
            return prepareResult
        }
        override fun commit(xid: Xid, onePhase: Boolean) {
            calls.add(XACall("commit", xid, onePhase = onePhase))
        }
        override fun rollback(xid: Xid) {
            calls.add(XACall("rollback", xid))
        }
        override fun recover(flag: Int): Array<Xid> = emptyArray()
        override fun isSameRM(xares: XAResource): Boolean = false
        override fun getTransactionTimeout(): Int = 0
        override fun setTransactionTimeout(seconds: Int): Boolean = false
        override fun forget(xid: Xid) {}
    }
}
