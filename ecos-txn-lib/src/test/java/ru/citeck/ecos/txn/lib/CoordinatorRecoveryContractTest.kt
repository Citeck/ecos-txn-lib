package ru.citeck.ecos.txn.lib

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import ru.citeck.ecos.micrometer.EcosMicrometerContext
import ru.citeck.ecos.test.commons.EcosWebAppApiMock
import ru.citeck.ecos.txn.lib.action.TxnActionId
import ru.citeck.ecos.txn.lib.action.TxnActionType
import ru.citeck.ecos.txn.lib.commit.RecoveryData
import ru.citeck.ecos.txn.lib.commit.TxnCommitData
import ru.citeck.ecos.txn.lib.commit.repo.NoopTwoPhaseCommitRepo
import ru.citeck.ecos.txn.lib.commit.repo.TwoPhaseCommitRepo
import ru.citeck.ecos.txn.lib.commit.repo.TwoPhaseCommitStatus
import ru.citeck.ecos.txn.lib.manager.EcosTxnProps
import ru.citeck.ecos.txn.lib.manager.TransactionManagerImpl
import ru.citeck.ecos.txn.lib.manager.api.client.ApiVersionRes
import ru.citeck.ecos.txn.lib.manager.api.client.TxnManagerRemoteApiClient
import ru.citeck.ecos.txn.lib.transaction.TransactionImpl
import ru.citeck.ecos.txn.lib.transaction.TxnId
import java.lang.reflect.Proxy

class CoordinatorRecoveryContractTest {

    private val managers = mutableListOf<TransactionManagerImpl>()

    @AfterEach
    fun tearDown() {
        managers.forEach { it.shutdown() }
    }

    @Test
    fun `own and global scans stay bounded when a failing journal record repeats`() {
        for (own in listOf(true, false)) {
            val fixture = Fixture()
            fixture.repo.entries.add(fixture.data())
            fixture.repo.own = own
            fixture.failTargets.add("worker")
            fixture.run()
            val queries = if (own) fixture.repo.ownQueries.size else fixture.repo.globalQueries
            assertThat(queries).isBetween(2, 11)
            assertThat(fixture.repo.acks.size).isBetween(1, 10)
            assertThat(fixture.repo.entries).hasSize(1)
            assertThat(fixture.calls).noneMatch { it.startsWith("dispose") || it.startsWith("action") }
        }
    }

    @Test
    fun `own scan excludes live transaction ids before recovering an eligible record`() {
        val fixture = Fixture()
        fixture.repo.own = true
        val live = fixture.data()
        fixture.repo.entries.add(live)
        val txn = TransactionImpl(live.txnId, "coordinator", false, EcosMicrometerContext.NOOP)
        txn.start()
        fixture.manager.transactionsById[live.txnId] = TransactionManagerImpl.TransactionInfo(txn)
        val eligible = fixture.data()
        fixture.repo.entries.add(eligible)
        fixture.run()
        assertThat(fixture.repo.ownQueries).anyMatch { live.txnId in it }
        assertThat(fixture.repo.entries).containsExactly(live)
        assertThat(fixture.recoveredIds).containsExactly(eligible.txnId)
        assertThat(fixture.manager.transactionsById).containsKey(live.txnId)
    }

    @Test
    fun `own scan bounds a long sequence of live ids without touching their resources`() {
        val fixture = Fixture()
        fixture.repo.own = true
        repeat(30) {
            val record = fixture.data()
            fixture.repo.entries.add(record)
            val txn = TransactionImpl(record.txnId, "coordinator", false, EcosMicrometerContext.NOOP)
            txn.start()
            fixture.manager.transactionsById[record.txnId] = TransactionManagerImpl.TransactionInfo(txn)
        }
        fixture.run()
        assertThat(fixture.repo.ownQueries.size).isBetween(2, 22)
        assertThat(fixture.repo.entries).hasSize(30)
        assertThat(fixture.calls).isEmpty()
    }

    @Test
    fun `alive route targets exact instance and executes only matching completion actions before dispose`() {
        for (committing in listOf(true, false)) {
            val fixture = Fixture()
            val type = if (committing) TxnActionType.AFTER_COMMIT else TxnActionType.AFTER_ROLLBACK
            val data = fixture.data(committing).let {
                it.copy(
                    data = it.data.copy(
                        actions = mapOf(
                            type to listOf(TxnActionId("worker", 7), TxnActionId("dead", 8), TxnActionId("dead", 9), TxnActionId("unrouted", 10)),
                            TxnActionType.BEFORE_COMMIT to listOf(TxnActionId("worker", 11))
                        )
                    ),
                    appRoutes = mapOf("worker" to "live", "dead" to "gone")
                )
            }
            fixture.unavailable.add("dead:gone")
            fixture.repo.entries.add(data)
            fixture.run()
            val operation = if (committing) "commit" else "rollback"
            assertThat(fixture.calls).containsSubsequence(
                "$operation:worker:live",
                "action:worker:live:7",
                "dispose:worker:live"
            )
            assertThat(fixture.calls.filter { it.startsWith("action:") }).containsExactly("action:worker:live:7")
            assertThat(fixture.calls.count { it == "available:dead:gone" }).isEqualTo(1)
            assertThat(fixture.repo.acks.single().processed).containsExactly("worker")
            assertThat(fixture.repo.entries).isEmpty()
        }
    }

    @Test
    fun `dead participant instance recovers by app name and skips its process-local actions`() {
        val fixture = Fixture()
        val data = fixture.data().let {
            it.copy(
                appRoutes = mapOf("worker" to "gone"),
                data = it.data.copy(actions = mapOf(TxnActionType.AFTER_COMMIT to listOf(TxnActionId("worker", 1))))
            )
        }
        fixture.unavailable.add("worker:gone")
        fixture.repo.entries.add(data)
        fixture.run()
        assertThat(fixture.calls).contains("commit:worker")
        assertThat(fixture.calls).noneMatch { it.startsWith("action:") || it.startsWith("dispose:") }
        assertThat(fixture.repo.entries).isEmpty()
    }

    @Test
    fun `partial recovery acknowledges successful apps and holds completion actions until all succeed`() {
        val fixture = Fixture()
        val base = fixture.data()
        fixture.repo.entries.add(
            base.copy(
                data = base.data.copy(
                    apps = linkedMapOf("worker" to emptySet(), "broken" to emptySet()),
                    actions = mapOf(TxnActionType.AFTER_COMMIT to listOf(TxnActionId("worker", 1)))
                ),
                appsToProcess = setOf("worker", "broken"),
                appRoutes = mapOf("worker" to "live")
            )
        )
        fixture.failTargets.add("broken")
        fixture.run()
        assertThat(fixture.repo.acks.first().processed).containsExactly("worker")
        assertThat(fixture.repo.acks.first().errors.keys).containsExactly("broken")
        assertThat(fixture.repo.entries.single().appsToProcess).containsExactly("broken")
        assertThat(fixture.calls).noneMatch { it.startsWith("action:") || it.startsWith("dispose:") }
        fixture.failTargets.clear()
        fixture.run()
        assertThat(fixture.repo.entries).isEmpty()
        assertThat(fixture.calls).containsSubsequence("commit:broken", "action:worker:live:1", "dispose:worker:live")
    }

    @Test
    fun `journal acknowledgement failure propagates before any completion action or disposal`() {
        val fixture = Fixture()
        fixture.repo.entries.add(
            fixture.data().let {
                it.copy(
                    appRoutes = mapOf("worker" to "live"),
                    data = it.data.copy(
                        actions = mapOf(TxnActionType.AFTER_COMMIT to listOf(TxnActionId("worker", 1)))
                    )
                )
            }
        )
        fixture.repo.ackFailure = IllegalStateException("journal write failed")
        assertThatThrownBy { fixture.run() }.isSameAs(fixture.repo.ackFailure)
        assertThat(fixture.calls).contains("commit:worker:live")
        assertThat(fixture.calls).noneMatch { it.startsWith("action:") || it.startsWith("dispose:") }
        assertThat(fixture.repo.entries).hasSize(1)
    }

    private inner class Fixture {
        val calls = mutableListOf<String>()
        val failTargets = mutableSetOf<String>()
        val unavailable = mutableSetOf<String>()
        val recoveredIds = mutableListOf<TxnId>()
        val repo = Journal()
        val manager = TransactionManagerImpl()

        init {
            val remote = Proxy.newProxyInstance(javaClass.classLoader, arrayOf(TxnManagerRemoteApiClient::class.java)) { _, method, args ->
                val app = args!![0] as String
                when (method.name) {
                    "isApiVersionSupported" -> ApiVersionRes.SUPPORTED
                    "isAppAvailable" -> {
                        calls.add("available:$app")
                        app !in unavailable
                    }
                    "recoveryCommit", "recoveryRollback" -> {
                        calls.add("${if (method.name == "recoveryCommit") "commit" else "rollback"}:$app")
                        recoveredIds.add(args[1] as TxnId)
                        check(app !in failTargets) { "recovery failed $app" }
                        Unit
                    }
                    "executeTxnAction" -> {
                        calls.add("action:$app:${args[2]}")
                        Unit
                    }
                    "disposeTxn" -> {
                        calls.add("dispose:$app")
                        Unit
                    }
                    else -> error("Unexpected remote operation ${method.name}")
                }
            } as TxnManagerRemoteApiClient
            manager.init(EcosWebAppApiMock("coordinator"), EcosTxnProps(), twoPhaseCommitRepo = repo, remoteClient = remote)
            managers.add(manager)
        }

        fun data(committing: Boolean = true): RecoveryData {
            return RecoveryData(
                TxnId.create("origin", "old"),
                TxnCommitData(mapOf("worker" to emptySet()), emptyMap()),
                if (committing) TwoPhaseCommitStatus.COMMITTING else TwoPhaseCommitStatus.PREPARING,
                setOf("worker"),
                "coordinator:old",
                emptyMap()
            )
        }

        fun run() {
            manager.commitCoordinator.runTxnRecovering()
        }
    }

    private data class Ack(val processed: Set<String>, val errors: Map<String, Throwable>)

    private class Journal : TwoPhaseCommitRepo by NoopTwoPhaseCommitRepo {
        val entries = mutableListOf<RecoveryData>()
        val ownQueries = mutableListOf<List<TxnId>>()
        var globalQueries = 0
        var own = false
        val acks = mutableListOf<Ack>()
        var ackFailure: Throwable? = null

        override fun findOwnDataToRecover(exclusions: List<TxnId>): RecoveryData? {
            ownQueries.add(exclusions.toList())
            return if (own) entries.firstOrNull { it.txnId !in exclusions } else null
        }

        override fun findDataToRecover(): RecoveryData? {
            globalQueries++
            return if (own) null else entries.firstOrNull()
        }

        override fun afterCommit(txnId: TxnId, committedApps: Set<String>, errors: Map<String, Throwable>) {
            acknowledge(txnId, committedApps, errors)
        }

        override fun afterRollback(txnId: TxnId, rolledBackApps: Set<String>, errors: Map<String, Throwable>) {
            acknowledge(txnId, rolledBackApps, errors)
        }

        private fun acknowledge(txnId: TxnId, processed: Set<String>, errors: Map<String, Throwable>) {
            ackFailure?.let { throw it }
            acks.add(Ack(processed.toSet(), errors.toMap()))
            val index = entries.indexOfFirst { it.txnId == txnId }
            val remaining = entries[index].appsToProcess - processed
            if (remaining.isEmpty()) {
                entries.removeAt(index)
            } else {
                entries[index] = entries[index].copy(appsToProcess = remaining)
            }
        }
    }
}
