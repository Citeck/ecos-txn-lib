package ru.citeck.ecos.txn.lib

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import ru.citeck.ecos.micrometer.EcosMicrometerContext
import ru.citeck.ecos.test.commons.EcosWebAppApiMock
import ru.citeck.ecos.txn.lib.commit.RecoveryData
import ru.citeck.ecos.txn.lib.commit.TxnCommitData
import ru.citeck.ecos.txn.lib.commit.repo.NoopTwoPhaseCommitRepo
import ru.citeck.ecos.txn.lib.commit.repo.TwoPhaseCommitRepo
import ru.citeck.ecos.txn.lib.commit.repo.TwoPhaseCommitStatus
import ru.citeck.ecos.txn.lib.manager.EcosTxnProps
import ru.citeck.ecos.txn.lib.manager.TransactionManagerImpl
import ru.citeck.ecos.txn.lib.manager.TxnManagerJob
import ru.citeck.ecos.txn.lib.resource.CommitPrepareStatus
import ru.citeck.ecos.txn.lib.resource.TransactionResource
import ru.citeck.ecos.txn.lib.transaction.ManagedTransaction
import ru.citeck.ecos.txn.lib.transaction.TransactionImpl
import ru.citeck.ecos.txn.lib.transaction.TransactionStatus
import ru.citeck.ecos.txn.lib.transaction.TxnId
import ru.citeck.ecos.txn.lib.transaction.ctx.NoopTxnManagerContext
import ru.citeck.ecos.txn.lib.transaction.xid.EcosXid
import java.time.Instant

class CommitDecisionSafetyTest {

    private val managers = mutableListOf<TransactionManagerImpl>()

    @AfterEach
    fun tearDown() {
        managers.forEach { it.shutdown() }
    }

    @Test
    fun `root commit errors preserve branches and recovery skips completed resources`() {
        for (durable in listOf(true, false)) {
            val journal = Journal()
            val manager = manager(if (durable) journal else NoopTwoPhaseCommitRepo)
            lateinit var txn: ManagedTransaction
            lateinit var resources: List<Resource>

            assertThatThrownBy {
                TxnContext.doInTxn {
                    txn = TxnContext.getTxn() as ManagedTransaction
                    resources = enlist(txn)
                }
            }.hasMessage("Transient commit failure")

            assertThat(txn.getStatus()).isEqualTo(TransactionStatus.COMMITTING)
            assertThat(resources.map { it.rollbacks }).containsExactly(0, 0)
            assertThat(resources.map { it.disposals }).containsExactly(0, 0)
            if (durable) {
                assertThat(journal.state!!.status).isEqualTo(TwoPhaseCommitStatus.COMMITTING)
                manager.commitCoordinator.runTxnRecovering()
                assertThat(journal.state).isNull()
            } else {
                manager.recoveryCommit(txn.getId(), resources.map { it.getXid() })
                manager.dispose(txn.getId())
            }
            assertThat(resources.map { it.commitAttempts }).containsExactly(1, 2)
            assertThat(resources.map { it.commits }).containsExactly(1, 1)
            assertThat(resources.map { it.rollbacks }).containsExactly(0, 0)
            assertThat(manager.getManagedTransactionOrNull(txn.getId())).isNull()
        }
    }

    @Test
    fun `partial prepared commit can be retried without repeating successful resources`() {
        val txn = preparedTransaction()
        val resources = txn.getResources().map { it as Resource }
        assertThatThrownBy { txn.commitPrepared() }.hasMessage("Transient commit failure")

        txn.commitPrepared()
        txn.commitPrepared()

        assertThat(txn.getStatus()).isEqualTo(TransactionStatus.COMMITTED)
        assertThat(resources.map { it.commitAttempts }).containsExactly(1, 2)
        assertThat(resources.map { it.commits }).containsExactly(1, 1)
    }

    @Test
    fun `rollback and direct disposal cannot reverse a pending commit`() {
        val txn = preparedTransaction()
        assertThatThrownBy { txn.commitPrepared() }.hasMessage("Transient commit failure")

        assertThatThrownBy { txn.rollback(null) }.isInstanceOf(IllegalStateException::class.java)
        assertThatThrownBy { txn.dispose() }.isInstanceOf(IllegalStateException::class.java)

        assertThat(txn.getStatus()).isEqualTo(TransactionStatus.COMMITTING)
        assertThat(txn.getResources().map { (it as Resource).rollbacks }).containsExactly(0, 0)
    }

    @Test
    fun `manager retains live transaction when disposal is rejected`() {
        val manager = manager(Journal())
        val txn = preparedTransaction()
        manager.transactionsById[txn.getId()] = TransactionManagerImpl.TransactionInfo(txn)
        assertThatThrownBy { txn.commitPrepared() }.hasMessage("Transient commit failure")

        assertThatThrownBy { manager.dispose(txn.getId()) }.isInstanceOf(IllegalStateException::class.java)
        assertThat(manager.getManagedTransactionOrNull(txn.getId())).isSameAs(txn)
        manager.recoveryCommit(txn.getId(), txn.getResources().map { it.getXid() })
        manager.dispose(txn.getId())
        assertThat(manager.getManagedTransactionOrNull(txn.getId())).isNull()
    }

    @Test
    fun `inactivity cleanup preserves pending commits and coordinator-owned outcome evidence`() {
        for ((status, recoverable) in listOf(
            TransactionStatus.COMMITTING to false,
            TransactionStatus.COMMITTING to true,
            TransactionStatus.COMMITTED to true,
            TransactionStatus.ROLLED_BACK to true,
            TransactionStatus.ROLLING_BACK to true
        )) {
            val manager = manager(Journal())
            val txn = preparedTransaction(rollbackFailures = if (status == TransactionStatus.ROLLING_BACK) 1 else 0)
            when (status) {
                TransactionStatus.COMMITTING, TransactionStatus.COMMITTED -> {
                    assertThatThrownBy { txn.commitPrepared() }.hasMessage("Transient commit failure")
                    if (status == TransactionStatus.COMMITTED) {
                        txn.commitPrepared()
                    }
                }
                TransactionStatus.ROLLED_BACK -> txn.rollback(null)
                else -> assertThatThrownBy { txn.rollback(null) }.isInstanceOf(RuntimeException::class.java)
            }
            var disposals = 0
            val idleTxn = object : ManagedTransaction by txn {
                override fun getLastActiveTime(): Instant {
                    return Instant.EPOCH
                }

                override fun dispose() {
                    disposals++
                    txn.dispose()
                }
            }
            manager.transactionsById[txn.getId()] = TransactionManagerImpl.TransactionInfo(
                idleTxn,
                managerCanRecoverPreparedTxn = recoverable,
                lastAliveTime = 0L
            )
            val outcomes = txn.getResources().map { (it as Resource).rollbacks to it.commits }
            val xids = txn.getResources().map { it.getXid() }
            val resources = txn.getResources().map { it as Resource }
            val job = TxnManagerJob(manager)
            val cleanup = TxnManagerJob::class.java.getDeclaredMethod("disposeInactiveTransactions")
            cleanup.isAccessible = true

            cleanup.invoke(job)

            if (status == TransactionStatus.COMMITTED || status == TransactionStatus.ROLLED_BACK) {
                assertThat(disposals).isEqualTo(1)
                assertThat(manager.getManagedTransactionOrNull(txn.getId())).isNull()
                val marker = listOf(EcosXid.create(txn.getId(), ByteArray(0)))
                if (status == TransactionStatus.COMMITTED) {
                    manager.recoveryCommit(txn.getId(), xids)
                    manager.recoveryCommit(txn.getId(), marker)
                    assertThatThrownBy { manager.recoveryRollback(txn.getId(), xids) }
                        .isInstanceOf(IllegalStateException::class.java)
                } else {
                    manager.recoveryRollback(txn.getId(), xids)
                    manager.recoveryRollback(txn.getId(), marker)
                    assertThatThrownBy { manager.recoveryCommit(txn.getId(), xids) }
                        .isInstanceOf(IllegalStateException::class.java)
                }
            } else {
                assertThat(disposals).isZero()
                assertThat(manager.getManagedTransactionOrNull(txn.getId())).isSameAs(idleTxn)
            }
            assertThat(resources.map { it.rollbacks to it.commits }).isEqualTo(outcomes)
        }
    }

    @Test
    fun `journal failures recover the stored decision without guessing rollback`() {
        for ((fault, failures) in listOf(
            Fault.BEFORE_COMMIT_WRITE to 0,
            Fault.AFTER_COMMIT_WRITE to 0,
            Fault.AFTER_COMMIT_UPDATE to 0,
            Fault.AFTER_COMMIT_UPDATE to 1
        )) {
            val journal = Journal(fault)
            val manager = manager(journal)
            lateinit var txn: ManagedTransaction
            lateinit var resources: List<Resource>
            assertThatThrownBy {
                TxnContext.doInTxn {
                    txn = TxnContext.getTxn() as ManagedTransaction
                    resources = enlist(txn, commitFailures = failures)
                }
            }.hasMessage("Journal failure")
            assertThat(resources.map { it.rollbacks }).containsExactly(0, 0)
            assertThat(manager.getManagedTransactionOrNull(txn.getId())).isSameAs(txn)

            manager.commitCoordinator.runTxnRecovering()

            assertThat(journal.state).isNull()
            if (fault == Fault.BEFORE_COMMIT_WRITE) {
                assertThat(resources.map { it.commits }).containsExactly(0, 0)
                assertThat(resources.map { it.rollbacks }).containsExactly(1, 1)
            } else {
                assertThat(resources.map { it.commits }).containsExactly(1, 1)
                assertThat(resources.map { it.rollbacks }).containsExactly(0, 0)
            }
            assertThat(manager.getManagedTransactionOrNull(txn.getId())).isNull()
        }
    }

    @Test
    fun `pre-prepare journal failure releases active resources safely`() {
        val manager = manager(Journal(Fault.BEFORE_PREPARE_WRITE))
        lateinit var txn: ManagedTransaction
        lateinit var resources: List<Resource>
        assertThatThrownBy {
            TxnContext.doInTxn {
                txn = TxnContext.getTxn() as ManagedTransaction
                resources = enlist(txn)
            }
        }.hasMessage("Journal failure")

        assertThat(resources.map { it.commits }).containsExactly(0, 0)
        assertThat(resources.map { it.rollbacks }).containsExactly(1, 1)
        assertThat(manager.getManagedTransactionOrNull(txn.getId())).isNull()
    }

    @Test
    fun `prepare failure still rolls back when writing the rollback decision fails`() {
        val manager = manager(Journal(Fault.BEFORE_ROLLBACK_WRITE))
        lateinit var txn: ManagedTransaction
        lateinit var resources: List<Resource>
        val error = org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException::class.java) {
            TxnContext.doInTxn {
                txn = TxnContext.getTxn() as ManagedTransaction
                resources = enlist(txn, failOnPrepare = true)
            }
        }
        assertThat(error).hasMessage("Prepare failure")
        assertThat(error.suppressed.map { it.message }).contains("Journal failure")

        assertThat(resources.map { it.commits }).containsExactly(0, 0)
        assertThat(resources.map { it.rollbacks }).containsExactly(1, 1)
        assertThat(manager.getManagedTransactionOrNull(txn.getId())).isNull()
    }

    @Test
    fun `confirmed prepare rollback retires the journal before disposing its evidence`() {
        val journal = Journal()
        val manager = manager(journal)
        lateinit var resources: List<Resource>
        assertThatThrownBy {
            TxnContext.doInTxn { resources = enlist(TxnContext.getTxn() as ManagedTransaction, failOnPrepare = true) }
        }.hasMessage("Prepare failure")

        assertThat(resources.map { it.rollbacks }).containsExactly(1, 1)
        assertThat(journal.state).isNull()
        manager.commitCoordinator.runTxnRecovering()
        assertThat(journal.state).isNull()
    }

    @Test
    fun `lost rollback acknowledgement retains live outcome until journal recovery`() {
        val journal = Journal(Fault.AFTER_ROLLBACK_UPDATE)
        val manager = manager(journal)
        lateinit var txn: ManagedTransaction
        lateinit var resources: List<Resource>
        assertThatThrownBy {
            TxnContext.doInTxn {
                txn = TxnContext.getTxn() as ManagedTransaction
                resources = enlist(txn, failOnPrepare = true)
            }
        }.hasMessage("Prepare failure")
        assertThat(journal.state).isNotNull()
        assertThat(manager.getManagedTransactionOrNull(txn.getId())).isSameAs(txn)

        manager.commitCoordinator.runTxnRecovering()

        assertThat(journal.state).isNull()
        assertThat(resources.map { it.rollbacks }).containsExactly(1, 1)
        assertThat(manager.getManagedTransactionOrNull(txn.getId())).isNull()
    }

    @Test
    fun `uncertain prepare rollback remains retryable without repeating completed branches`() {
        val journal = Journal()
        val manager = manager(journal)
        lateinit var txn: ManagedTransaction
        lateinit var resources: List<Resource>
        assertThatThrownBy {
            TxnContext.doInTxn {
                txn = TxnContext.getTxn() as ManagedTransaction
                resources = enlist(txn, failOnPrepare = true, rollbackFailures = 1)
            }
        }.hasMessage("Prepare failure")
        assertThat(journal.state).isNotNull()
        assertThat(txn.getStatus()).isEqualTo(TransactionStatus.ROLLING_BACK)
        assertThat(manager.getManagedTransactionOrNull(txn.getId())).isSameAs(txn)

        manager.commitCoordinator.runTxnRecovering()

        assertThat(journal.state).isNull()
        assertThat(resources.map { it.rollbackAttempts }).containsExactly(1, 2)
        assertThat(resources.map { it.rollbacks }).containsExactly(1, 1)
        assertThat(resources.map { it.commits }).containsExactly(0, 0)
    }

    @Test
    fun `failed one phase commit is not retried as a prepared commit`() {
        val txn = TransactionImpl(TxnId.create("app", "instance"), "app", false, EcosMicrometerContext.NOOP)
        txn.start()
        txn.doWithinTxn(NoopTxnManagerContext, false) {
            txn.getOrAddRes("only") { _, id -> Resource(id, "only", 1) }
        }
        assertThatThrownBy { txn.onePhaseCommit() }.hasMessage("Transient commit failure")

        assertThatThrownBy { txn.commitPrepared() }.isInstanceOf(IllegalStateException::class.java)
        assertThat((txn.getResources().single() as Resource).commitAttempts).isEqualTo(1)
    }

    private fun manager(journal: TwoPhaseCommitRepo): TransactionManagerImpl {
        val manager = TransactionManagerImpl()
        manager.init(EcosWebAppApiMock("app"), EcosTxnProps(), twoPhaseCommitRepo = journal)
        managers.add(manager)
        TxnContext.setManager(manager)
        return manager
    }

    private fun preparedTransaction(rollbackFailures: Int = 0): TransactionImpl {
        val txn = TransactionImpl(TxnId.create("app", "instance"), "app", false, EcosMicrometerContext.NOOP)
        txn.start()
        txn.doWithinTxn(NoopTxnManagerContext, false) { enlist(txn, rollbackFailures = rollbackFailures) }
        txn.prepareCommit()
        return txn
    }

    private fun enlist(
        txn: ManagedTransaction,
        failOnPrepare: Boolean = false,
        commitFailures: Int = 1,
        rollbackFailures: Int = 0
    ): List<Resource> {
        return listOf(
            txn.getOrAddRes("first") { _, id -> Resource(id, "first", 0) },
            txn.getOrAddRes("second") { _, id -> Resource(id, "second", commitFailures, failOnPrepare, rollbackFailures) }
        )
    }

    private enum class Fault {
        NONE,
        BEFORE_PREPARE_WRITE,
        BEFORE_COMMIT_WRITE,
        AFTER_COMMIT_WRITE,
        AFTER_COMMIT_UPDATE,
        BEFORE_ROLLBACK_WRITE,
        AFTER_ROLLBACK_UPDATE
    }

    private class Journal(private var fault: Fault = Fault.NONE) : TwoPhaseCommitRepo by NoopTwoPhaseCommitRepo {
        var state: RecoveryData? = null

        override fun beforePrepare(txnId: TxnId, data: TxnCommitData) {
            failAt(Fault.BEFORE_PREPARE_WRITE)
            state = RecoveryData(txnId, data, TwoPhaseCommitStatus.PREPARING, data.apps.keys, "app", emptyMap())
        }

        override fun beforeCommit(txnId: TxnId, appsToCommit: Set<String>) {
            failAt(Fault.BEFORE_COMMIT_WRITE)
            state = state!!.copy(status = TwoPhaseCommitStatus.COMMITTING, appsToProcess = appsToCommit)
            failAt(Fault.AFTER_COMMIT_WRITE)
        }

        override fun beforeRollback(txnId: TxnId, appsToRollback: Set<String>) {
            failAt(Fault.BEFORE_ROLLBACK_WRITE)
            state = state?.copy(status = TwoPhaseCommitStatus.ROLLING_BACK, appsToProcess = appsToRollback)
        }

        override fun afterCommit(txnId: TxnId, committedApps: Set<String>, errors: Map<String, Throwable>) {
            failAt(Fault.AFTER_COMMIT_UPDATE)
            if (errors.isEmpty()) {
                state = null
            }
        }

        override fun afterRollback(txnId: TxnId, rolledBackApps: Set<String>, errors: Map<String, Throwable>) {
            failAt(Fault.AFTER_ROLLBACK_UPDATE)
            if (errors.isEmpty()) {
                state = null
            }
        }

        override fun getRecoveryData(txnId: TxnId): RecoveryData? {
            return state?.takeIf { it.txnId == txnId }
        }

        override fun findOwnDataToRecover(exclusions: List<TxnId>): RecoveryData? {
            return state?.takeUnless { it.txnId in exclusions }
        }

        private fun failAt(point: Fault) {
            if (fault == point) {
                fault = Fault.NONE
                error("Journal failure")
            }
        }
    }

    private class Resource(
        private val txnId: TxnId,
        private val name: String,
        private var failures: Int,
        private val failOnPrepare: Boolean = false,
        private var rollbackFailures: Int = 0
    ) : TransactionResource {
        var commitAttempts = 0
        var commits = 0
        var rollbacks = 0
        var rollbackAttempts = 0
        var disposals = 0

        override fun getName(): String {
            return name
        }

        override fun getXid(): EcosXid {
            return EcosXid.create(txnId, name.toByteArray())
        }

        override fun start() {}
        override fun end() {}

        override fun prepareCommit(): CommitPrepareStatus {
            check(!failOnPrepare) { "Prepare failure" }
            return CommitPrepareStatus.PREPARED
        }

        override fun commitPrepared() {
            commitAttempts++
            if (failures > 0) {
                failures--
                error("Transient commit failure")
            }
            commits++
        }

        override fun onePhaseCommit() {
            commitPrepared()
        }

        override fun rollback() {
            rollbackAttempts++
            if (rollbackFailures > 0) {
                rollbackFailures--
                error("Rollback failure")
            }
            rollbacks++
        }

        override fun dispose() {
            disposals++
        }
    }
}
