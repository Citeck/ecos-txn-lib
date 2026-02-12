package ru.citeck.ecos.txn.lib

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.txn.lib.manager.recovery.RecoverableStorage
import ru.citeck.ecos.txn.lib.manager.recovery.RecoveryManagerImpl
import ru.citeck.ecos.txn.lib.transaction.TxnId
import ru.citeck.ecos.txn.lib.transaction.xid.EcosXid

class RecoveryManagerTest {

    @Test
    fun `commitPrepared with empty xids does nothing`() {
        val storage = TestRecoverableStorage()
        val recovery = RecoveryManagerImpl()
        recovery.registerStorage(storage)

        recovery.commitPrepared(emptyList())

        assertThat(storage.committed).isEmpty()
    }

    @Test
    fun `commitPrepared delegates to correct storage`() {
        val txnId = TxnId.create("app", "inst")
        val xid = EcosXid.create(txnId, "app", "inst")

        val storage = TestRecoverableStorage(preparedXids = mutableListOf(xid))
        val recovery = RecoveryManagerImpl()
        recovery.registerStorage(storage)

        recovery.commitPrepared(listOf(xid))

        assertThat(storage.committed).containsExactly(xid)
    }

    @Test
    fun `rollbackPrepared delegates to correct storage`() {
        val txnId = TxnId.create("app", "inst")
        val xid = EcosXid.create(txnId, "app", "inst")

        val storage = TestRecoverableStorage(preparedXids = mutableListOf(xid))
        val recovery = RecoveryManagerImpl()
        recovery.registerStorage(storage)

        recovery.rollbackPrepared(listOf(xid))

        assertThat(storage.rolledBack).containsExactly(xid)
    }

    @Test
    fun `commitPrepared with unknown xid is ignored`() {
        val txnId1 = TxnId.create("app", "inst")
        val xid1 = EcosXid.create(txnId1, "app", "inst")

        val txnId2 = TxnId.create("app2", "inst2")
        val unknownXid = EcosXid.create(txnId2, "app2", "inst2")

        val storage = TestRecoverableStorage(preparedXids = mutableListOf(xid1))
        val recovery = RecoveryManagerImpl()
        recovery.registerStorage(storage)

        recovery.commitPrepared(listOf(unknownXid))

        assertThat(storage.committed).isEmpty()
    }

    @Test
    fun `rollbackPrepared with empty xids does nothing`() {
        val storage = TestRecoverableStorage()
        val recovery = RecoveryManagerImpl()
        recovery.registerStorage(storage)

        recovery.rollbackPrepared(emptyList())

        assertThat(storage.rolledBack).isEmpty()
    }

    @Test
    fun `multiple storages resolve xids from correct storage`() {
        val txnId1 = TxnId.create("app1", "inst1")
        val xid1 = EcosXid.create(txnId1, "app1", "inst1")

        val txnId2 = TxnId.create("app2", "inst2")
        val xid2 = EcosXid.create(txnId2, "app2", "inst2")

        val storage1 = TestRecoverableStorage(preparedXids = mutableListOf(xid1))
        val storage2 = TestRecoverableStorage(preparedXids = mutableListOf(xid2))

        val recovery = RecoveryManagerImpl()
        recovery.registerStorage(storage1)
        recovery.registerStorage(storage2)

        recovery.commitPrepared(listOf(xid1, xid2))

        assertThat(storage1.committed).containsExactly(xid1)
        assertThat(storage2.committed).containsExactly(xid2)
        assertThat(storage1.rolledBack).isEmpty()
        assertThat(storage2.rolledBack).isEmpty()
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
