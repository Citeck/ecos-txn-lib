package ru.citeck.ecos.txn.lib

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
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
    fun `absent branch after complete scan is an idempotent commit retry`() {
        val txnId1 = TxnId.create("app", "inst")
        val xid1 = EcosXid.create(txnId1, "app", "inst")

        val txnId2 = TxnId.create("app2", "inst2")
        val unknownXid = EcosXid.create(txnId2, "app2", "inst2")

        val storage = TestRecoverableStorage(preparedXids = mutableListOf(xid1))
        val recovery = RecoveryManagerImpl()
        recovery.registerStorage(storage)

        recovery.commitPrepared(listOf(unknownXid))
        recovery.commitPrepared(listOf(unknownXid))

        assertThat(storage.committed).isEmpty()
        assertThat(storage.getPreparedXids()).containsExactly(xid1)
        assertThatThrownBy { recovery.rollbackPrepared(listOf(unknownXid)) }
            .hasMessageContaining("Conflicting recovery decision")
    }

    @Test
    fun `recovery completes late branches of the same transaction only`() {
        for (commit in listOf(true, false)) {
            val txnId = TxnId.create("app", "inst")
            val original = EcosXid.create(txnId, "original".toByteArray())
            val late = EcosXid.create(txnId, "late".toByteArray())
            val unrelated = EcosXid.create(TxnId.create("other", "inst"), "other".toByteArray())
            val storage = TestRecoverableStorage(mutableListOf(original, late, unrelated))
            val recovery = RecoveryManagerImpl()
            recovery.registerStorage(storage)

            if (commit) {
                recovery.commitPrepared(listOf(original))
                assertThat(storage.committed).containsExactlyInAnyOrder(original, late)
            } else {
                recovery.rollbackPrepared(listOf(original))
                assertThat(storage.rolledBack).containsExactlyInAnyOrder(original, late)
            }
            assertThat(storage.getPreparedXids()).containsExactly(unrelated)
        }
    }

    @Test
    fun `successful completion can be retried but opposite decision is rejected`() {
        val xid = EcosXid.create(TxnId.create("app", "inst"), "branch".toByteArray())
        val storage = TestRecoverableStorage(mutableListOf(xid))
        val recovery = RecoveryManagerImpl()
        recovery.registerStorage(storage)
        recovery.commitPrepared(listOf(xid))
        recovery.commitPrepared(listOf(xid))
        assertThat(storage.committed).containsExactly(xid)
        assertThatThrownBy { recovery.rollbackPrepared(listOf(xid)) }
            .hasMessageContaining("Conflicting recovery decision")
        assertThatThrownBy { recovery.rollbackPrepared(listOf(EcosXid.create(xid.getTransactionId(), ByteArray(0)))) }
            .hasMessageContaining("Conflicting recovery decision")

        val restarted = RecoveryManagerImpl()
        restarted.registerStorage(storage)
        restarted.commitPrepared(listOf(xid))
        assertThat(storage.committed).containsExactly(xid)
        assertThatThrownBy { restarted.rollbackPrepared(listOf(xid)) }
            .hasMessageContaining("Conflicting recovery decision")
    }

    @Test
    fun `known global completion rejects opposite marker and late branch decisions before storage changes`() {
        for (commit in listOf(true, false)) {
            for (markerRequest in listOf(true, false)) {
                val txnId = TxnId.create("app", "inst")
                val original = EcosXid.create(txnId, "original".toByteArray())
                val late = EcosXid.create(txnId, "late".toByteArray())
                val marker = EcosXid.create(txnId, ByteArray(0))
                val recovery = RecoveryManagerImpl()
                val storage = TestRecoverableStorage(mutableListOf(late))
                recovery.registerStorage(storage)
                recovery.rememberCompleted(txnId, listOf(original), commit)
                val request = listOf(if (markerRequest) marker else late)

                assertThatThrownBy {
                    if (commit) recovery.rollbackPrepared(request) else recovery.commitPrepared(request)
                }.hasMessageContaining("Conflicting recovery decision")

                assertThat(storage.committed).isEmpty()
                assertThat(storage.rolledBack).isEmpty()
                assertThat(storage.getPreparedXids()).containsExactly(late)
            }
        }
    }

    @Test
    fun `stateless global marker is idempotent without XA sources and rejects a known opposite decision`() {

        val marker = EcosXid.create(TxnId.create("app", "inst"), ByteArray(0))
        val recovery = RecoveryManagerImpl()
        recovery.commitPrepared(listOf(marker))
        RecoveryManagerImpl().commitPrepared(listOf(marker))
        assertThatThrownBy { recovery.rollbackPrepared(listOf(marker)) }
            .hasMessageContaining("Conflicting recovery decision")
    }

    @Test
    fun `recovery without registered storages fails closed`() {
        val xid = EcosXid.create(TxnId.create("app", "inst"), "branch".toByteArray())
        assertThatThrownBy { RecoveryManagerImpl().rollbackPrepared(listOf(xid)) }
            .hasMessageContaining("No recoverable storages")
    }

    @Test
    fun `partial storage failure can be retried without repeating a successful branch`() {
        val txnId = TxnId.create("app", "inst")
        val xids = setOf(
            EcosXid.create(txnId, "a".toByteArray()),
            EcosXid.create(txnId, "b".toByteArray())
        )
        val remaining = xids.toMutableSet()
        val committed = mutableListOf<EcosXid>()
        var calls = 0
        val recovery = RecoveryManagerImpl()
        recovery.registerStorage(object : RecoverableStorage {
            override fun getPreparedXids(): List<EcosXid> {
                return remaining.toList()
            }

            override fun commitPrepared(xid: EcosXid) {
                if (++calls == 2) {
                    error("Temporary storage failure")
                }
                remaining.remove(xid)
                committed.add(xid)
            }

            override fun rollbackPrepared(xid: EcosXid) {
                error("Unexpected rollback")
            }
        })

        assertThatThrownBy { recovery.commitPrepared(xids) }.hasMessage("Temporary storage failure")
        recovery.commitPrepared(xids)
        assertThat(committed).containsExactlyInAnyOrderElementsOf(xids)
        assertThat(remaining).isEmpty()
    }

    @Test
    fun `visible branches complete and absent branches acknowledge the repeated decision`() {
        for (commit in listOf(true, false)) {
            val txnId = TxnId.create("app", "inst")
            val visible = EcosXid.create(txnId, "visible".toByteArray())
            val missing = EcosXid.create(txnId, "missing".toByteArray())
            val storage = TestRecoverableStorage(mutableListOf(visible))
            val recovery = RecoveryManagerImpl()
            recovery.registerStorage(storage)

            repeat(2) {
                if (commit) {
                    recovery.commitPrepared(listOf(visible, missing))
                } else {
                    recovery.rollbackPrepared(listOf(visible, missing))
                }
            }

            assertThat(storage.getPreparedXids()).isEmpty()
            assertThat(if (commit) storage.committed else storage.rolledBack).containsExactly(visible)
            assertThat(if (commit) storage.rolledBack else storage.committed).isEmpty()
        }
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

    @Test
    fun `all registered sources must enumerate before completing any visible branch`() {

        for (commit in listOf(true, false)) {
            val xid = EcosXid.create(TxnId.create("app", "inst"), "visible".toByteArray())
            val visible = TestRecoverableStorage(mutableListOf(xid))
            val recovery = RecoveryManagerImpl()
            var failing = true
            recovery.registerStorage(visible)
            recovery.registerStorage(object : RecoverableStorage {
                override fun getPreparedXids(): List<EcosXid> {
                    check(!failing) { "Source scan failed" }
                    return emptyList()
                }

                override fun commitPrepared(xid: EcosXid) {
                    error("No branch in this source")
                }

                override fun rollbackPrepared(xid: EcosXid) {
                    error("No branch in this source")
                }
            })

            assertThatThrownBy {
                if (commit) recovery.commitPrepared(listOf(xid)) else recovery.rollbackPrepared(listOf(xid))
            }.hasMessage("Source scan failed")
            assertThat(visible.getPreparedXids()).containsExactly(xid)
            assertThat(visible.committed).isEmpty()
            assertThat(visible.rolledBack).isEmpty()

            failing = false
            if (commit) recovery.commitPrepared(listOf(xid)) else recovery.rollbackPrepared(listOf(xid))
            assertThat(visible.getPreparedXids()).isEmpty()
            assertThat(if (commit) visible.committed else visible.rolledBack).containsExactly(xid)
        }
    }

    private class TestRecoverableStorage(
        private val preparedXids: MutableList<EcosXid> = mutableListOf()
    ) : RecoverableStorage {

        val committed = mutableListOf<EcosXid>()
        val rolledBack = mutableListOf<EcosXid>()

        override fun getPreparedXids(): List<EcosXid> = preparedXids

        override fun commitPrepared(xid: EcosXid) {
            committed.add(xid)
            preparedXids.remove(xid)
        }

        override fun rollbackPrepared(xid: EcosXid) {
            rolledBack.add(xid)
            preparedXids.remove(xid)
        }
    }
}
