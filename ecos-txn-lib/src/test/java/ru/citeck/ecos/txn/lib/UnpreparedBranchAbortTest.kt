package ru.citeck.ecos.txn.lib

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import ru.citeck.ecos.test.commons.EcosWebAppApiMock
import ru.citeck.ecos.txn.lib.manager.AbortedBranches
import ru.citeck.ecos.txn.lib.manager.EcosTxnProps
import ru.citeck.ecos.txn.lib.manager.TransactionManagerImpl
import ru.citeck.ecos.txn.lib.manager.TransactionPolicy
import ru.citeck.ecos.txn.lib.resource.CommitPrepareStatus
import ru.citeck.ecos.txn.lib.resource.TransactionResource
import ru.citeck.ecos.txn.lib.transaction.TransactionStatus
import ru.citeck.ecos.txn.lib.transaction.TxnId
import ru.citeck.ecos.txn.lib.transaction.ctx.NoopTxnManagerContext
import ru.citeck.ecos.txn.lib.transaction.xid.EcosXid
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.concurrent.thread

/**
 * A branch that has not voted may be rolled back by its own app, which makes commit impossible
 * for the coordinator. The rollback must not interleave with a prepare, a late prepare must
 * vote no even in a context the web layer recreated for it, and no dispose may remove the
 * rolled-back branch before its tombstone exists.
 */
class UnpreparedBranchAbortTest {

    private val managers = mutableListOf<TransactionManagerImpl>()
    private val events = CopyOnWriteArrayList<String>()

    @AfterEach
    fun tearDown() {
        managers.forEach { it.shutdown() }
    }

    @Test
    fun `abort waits for a running prepare and leaves the prepared branch alone`() {
        val manager = createManager()
        val prepareEntered = CountDownLatch(1)
        val prepareReleased = CountDownLatch(1)
        val txnId = addBranch(manager) {
            prepareEntered.countDown()
            prepareReleased.await(10, TimeUnit.SECONDS)
        }

        val prepare = thread { manager.prepareCommitFromExtManager(txnId, true) }
        assertThat(prepareEntered.await(10, TimeUnit.SECONDS)).isTrue()
        var aborted: Boolean? = null
        val abort = thread { aborted = manager.abortUnpreparedBranch(txnId, RuntimeException("lost")) }
        awaitState(abort, Thread.State.BLOCKED)
        prepareReleased.countDown()
        prepare.join(10_000)
        abort.join(10_000)

        assertThat(aborted).isFalse()
        assertThat(manager.getStatus(txnId)).isEqualTo(TransactionStatus.PREPARED)
        assertThat(events).doesNotContain("res:rollback")
    }

    @Test
    fun `prepare after the abort is refused in the live and in a recreated context`() {
        val manager = createManager()
        val txnId = addBranch(manager)

        assertThat(manager.abortUnpreparedBranch(txnId, RuntimeException("lost"))).isTrue()
        assertThatThrownBy { manager.prepareCommitFromExtManager(txnId, true) }
            .hasMessageContaining("rolled back before prepare")
        manager.dispose(txnId)
        assertThatThrownBy { prepareInRecreatedContext(manager, txnId) }
            .hasMessageContaining("rolled back before prepare")

        assertThat(events).containsExactly("res:rollback")
    }

    @Test
    fun `dispose waits for the tombstone of an abort in progress`() {
        val manager = createManager()
        val txnId = addBranch(manager)
        var aborted: Boolean? = null
        val abort = thread(start = false) { aborted = manager.abortUnpreparedBranch(txnId, RuntimeException("lost")) }
        val dispose = thread(start = false) { manager.dispose(txnId) }

        // holding the tombstones stops the abort right after its rollback, inside the critical section
        synchronized(manager.abortedBranches) {
            abort.start()
            awaitState(abort, Thread.State.BLOCKED)
            assertThat(manager.getStatus(txnId)).isEqualTo(TransactionStatus.ROLLED_BACK)
            dispose.start()
            awaitState(dispose, Thread.State.BLOCKED)
            assertThat(manager.getStatus(txnId)).isEqualTo(TransactionStatus.ROLLED_BACK)
        }
        abort.join(10_000)
        dispose.join(10_000)

        assertThat(aborted).isTrue()
        assertThat(manager.getStatus(txnId)).isEqualTo(TransactionStatus.NO_TRANSACTION)
        assertThat(events).containsExactly("res:rollback")
        assertThatThrownBy { prepareInRecreatedContext(manager, txnId) }
            .hasMessageContaining("rolled back before prepare")
    }

    @Test
    fun `inactivity disposal leaves a branch prepared after the job checked it to the coordinator`() {
        val manager = createManager()
        val txnId = addBranch(manager)
        manager.prepareCommitFromExtManager(txnId, true)

        manager.disposeInactive(txnId)

        assertThat(manager.getStatus(txnId)).isEqualTo(TransactionStatus.PREPARED)
        assertThat(events).doesNotContain("res:rollback")
    }

    @Test
    fun `inactivity disposal of an unprepared branch refuses a late prepare`() {
        val manager = createManager()
        val txnId = addBranch(manager)

        manager.disposeInactive(txnId)

        assertThat(manager.getStatus(txnId)).isEqualTo(TransactionStatus.NO_TRANSACTION)
        assertThat(events).containsExactly("res:rollback")
        assertThatThrownBy { prepareInRecreatedContext(manager, txnId) }
            .hasMessageContaining("rolled back before prepare")
    }

    @Test
    fun `aborted branches are forgotten by age and by count`() {
        var now = 0L
        val branches = AbortedBranches(ttlMs = 1_000, maxSize = 2, clock = { now })
        val ids = (1..3).map { TxnId.create("origin", "instance") }

        branches.add(ids[0])
        now = 10
        branches.add(ids[1])
        branches.add(ids[2])
        assertThat(ids.map { branches.contains(it) }).containsExactly(false, true, true)

        now = 1_011
        assertThat(ids.map { branches.contains(it) }).containsExactly(false, false, false)
    }

    private fun prepareInRecreatedContext(manager: TransactionManagerImpl, txnId: TxnId) = manager.doInExtTxn(txnId, NoopTxnManagerContext, TransactionPolicy.REQUIRED, false) {
        manager.prepareCommitFromExtManager(txnId, true)
    }

    private fun awaitState(thread: Thread, state: Thread.State) {
        val deadline = System.currentTimeMillis() + 10_000
        while (thread.state != state) {
            check(thread.isAlive) { "Thread has finished, expected $state" }
            check(System.currentTimeMillis() < deadline) { "Thread state is ${thread.state}, expected $state" }
            Thread.sleep(5)
        }
    }

    private fun createManager(): TransactionManagerImpl {
        val manager = TransactionManagerImpl()
        manager.init(EcosWebAppApiMock("participant"), EcosTxnProps())
        managers.add(manager)
        return manager
    }

    private fun addBranch(manager: TransactionManagerImpl, onPrepare: () -> Unit = {}): TxnId {
        val txnId = TxnId.create("origin", "instance")
        manager.doInExtTxn(txnId, NoopTxnManagerContext, TransactionPolicy.REQUIRED, false) {
            manager.getCurrentTransaction()!!.getOrAddRes("res") { _, id -> Resource(id, onPrepare) }
        }
        return txnId
    }

    private inner class Resource(private val txnId: TxnId, private val onPrepare: () -> Unit) : TransactionResource {
        override fun start() {}
        override fun end() {}
        override fun getName() = "res"
        override fun getXid() = EcosXid.create(txnId, "res".toByteArray())
        override fun prepareCommit(): CommitPrepareStatus {
            onPrepare()
            return CommitPrepareStatus.PREPARED
        }
        override fun commitPrepared() {
            events.add("res:commitPrepared")
        }
        override fun onePhaseCommit() {
            events.add("res:onePhaseCommit")
        }
        override fun rollback() {
            events.add("res:rollback")
        }
        override fun dispose() {}
    }
}
