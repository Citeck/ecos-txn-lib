package ru.citeck.ecos.txn.lib

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import ru.citeck.ecos.commons.data.DataValue
import ru.citeck.ecos.commons.json.Json
import ru.citeck.ecos.micrometer.EcosMicrometerContext
import ru.citeck.ecos.test.commons.EcosWebAppApiMock
import ru.citeck.ecos.txn.lib.commit.RecoveryData
import ru.citeck.ecos.txn.lib.commit.TxnCommitData
import ru.citeck.ecos.txn.lib.commit.repo.AuthoritativeTwoPhaseCommitRepo
import ru.citeck.ecos.txn.lib.commit.repo.JournalPresence
import ru.citeck.ecos.txn.lib.commit.repo.NoopTwoPhaseCommitRepo
import ru.citeck.ecos.txn.lib.commit.repo.TwoPhaseCommitRepo
import ru.citeck.ecos.txn.lib.commit.repo.TwoPhaseCommitStatus
import ru.citeck.ecos.txn.lib.manager.EcosTxnProps
import ru.citeck.ecos.txn.lib.manager.TransactionManagerImpl
import ru.citeck.ecos.txn.lib.manager.api.client.ApiVersionRes
import ru.citeck.ecos.txn.lib.manager.api.client.TxnManagerRemoteApiClient
import ru.citeck.ecos.txn.lib.manager.api.client.TxnManagerWebApiClient
import ru.citeck.ecos.txn.lib.manager.api.server.TxnManagerRemoteActions
import ru.citeck.ecos.txn.lib.manager.api.server.action.TmJournalPresenceAction
import ru.citeck.ecos.txn.lib.manager.recovery.RecoverableStorage
import ru.citeck.ecos.txn.lib.manager.recovery.RecoveryManagerImpl
import ru.citeck.ecos.txn.lib.manager.recovery.StartupOrphanRecovery
import ru.citeck.ecos.txn.lib.transaction.TransactionImpl
import ru.citeck.ecos.txn.lib.transaction.TxnId
import ru.citeck.ecos.txn.lib.transaction.xid.EcosXid
import ru.citeck.ecos.webapp.api.web.client.EcosWebClientApi
import java.lang.reflect.Proxy
import java.time.Duration
import java.time.Instant

class StartupOrphanRecoveryTest {

    private val now = Instant.parse("2026-10-20T10:00:00Z")
    private val old = TxnId.create("origin", "previous", now.minus(Duration.ofDays(31)))
    private val managers = mutableListOf<TransactionManagerImpl>()

    @AfterEach
    fun close() {
        managers.forEach { it.shutdown() }
    }

    @Test
    fun `delay starts at readiness and successful sweep never runs again`() {

        val storage = Storage(branch(old))
        val recovery = RecoveryManagerImpl()
        recovery.registerStorage(storage)
        var queries = 0
        val sweep = StartupOrphanRecovery(recovery) {
            queries++
            JournalPresence.ABSENT
        }
        sweep.runIfDue(now.plusSeconds(600))
        assertThat(storage.scans).isZero()
        sweep.start(now)
        sweep.start(now.plusSeconds(30))
        sweep.runIfDue(now.plusSeconds(59))
        assertThat(storage.scans).isZero()
        sweep.runIfDue(now.plusSeconds(60))
        assertThat(storage.rolledBack).containsExactly(branch(old))
        val completedScans = storage.scans
        storage.prepared.add(branch(TxnId.create("other", "instance", old.created)))
        sweep.runIfDue(now.plusSeconds(3600))
        assertThat(queries).isEqualTo(1)
        assertThat(storage.scans).isEqualTo(completedScans)
    }

    @Test
    fun `age is global creation time and cutoff is strictly older than thirty days`() {

        val attempt = now.plusSeconds(60)
        val ids = listOf(
            old,
            TxnId.create("origin", "instance", attempt.minus(Duration.ofDays(30))),
            TxnId.create("origin", "instance", now.minus(Duration.ofDays(1))),
            TxnId.create("origin", "instance", now.plus(Duration.ofDays(2))),
            TxnId.EMPTY,
            TxnId.create("", "instance", old.created),
            TxnId.create("origin", "", old.created),
            TxnId.create("origin", "instance", Instant.EPOCH)
        )
        val storage = Storage(*ids.map { branch(it) }.toTypedArray())
        val asked = mutableListOf<TxnId>()
        val sweep = sweep(storage) {
            asked.add(it)
            JournalPresence.ABSENT
        }
        sweep.start(now)
        sweep.runIfDue(attempt)
        assertThat(asked).containsExactly(old)
        assertThat(storage.rolledBack).containsExactly(branch(old))
    }

    @Test
    fun `any journal record prevents startup rollback regardless of status`() {

        for (status in TwoPhaseCommitStatus.entries) {
            val repo = Journal(RecoveryData(old, TxnCommitData(emptyMap(), emptyMap()), status, emptySet(), "another-instance", emptyMap()))
            val manager = manager(repo)
            val storage = Storage(branch(old))
            manager.getRecoveryManager().registerStorage(storage)
            val sweep = manager.createStartupOrphanRecovery()
            sweep.start(now)
            sweep.runIfDue(now.plusSeconds(60))
            assertThat(storage.rolledBack).isEmpty()
            assertThat(repo.reads).isEqualTo(1)
        }
    }

    @Test
    fun `absent authoritative local journal rolls back all registered sources`() {

        val manager = manager(Journal())
        val a = Storage(branch(old, "a"))
        val b = Storage(branch(old, "b"))
        manager.getRecoveryManager().registerStorage(a)
        manager.getRecoveryManager().registerStorage(b)
        val sweep = manager.createStartupOrphanRecovery()
        sweep.start(now)
        sweep.runIfDue(now.plusSeconds(60))
        assertThat(a.rolledBack).containsExactly(branch(old, "a"))
        assertThat(b.rolledBack).containsExactly(branch(old, "b"))
    }

    @Test
    fun `noop legacy and wrong authority cannot prove absence`() {

        val noop = manager(NoopTwoPhaseCommitRepo)
        val legacy = manager(object : TwoPhaseCommitRepo by NoopTwoPhaseCommitRepo {})
        val delegated = manager(Journal(), EcosTxnProps("other-coordinator"))
        for (manager in listOf(noop, legacy, delegated)) {
            assertThat(manager.getJournalPresence("coordinator", old)).isEqualTo(JournalPresence.UNKNOWN)
        }
        assertThat(manager(Journal()).getJournalPresence("wrong-target", old)).isEqualTo(JournalPresence.UNKNOWN)
        assertThat(manager(Journal()).getJournalPresence("coordinator", TxnId.EMPTY)).isEqualTo(JournalPresence.UNKNOWN)
        assertThat(DataValue.createObj().getAsNotNull(TmJournalPresenceAction.Resp::class.java).presence)
            .isEqualTo(JournalPresence.UNKNOWN)
    }

    @Test
    fun `remote action checks version and serializes explicit authoritative response`() {

        val manager = manager(Journal())
        val actions = TxnManagerRemoteActions(manager)
        val data = DataValue.create(TmJournalPresenceAction.Data("coordinator", old))
        assertThatThrownBy { actions.execute(TmJournalPresenceAction.TYPE, data, 2) }
            .hasMessageContaining("requires API v3")
        val result = actions.execute(TmJournalPresenceAction.TYPE, data, 3)
        assertThat(result!!.getAsNotNull(TmJournalPresenceAction.Resp::class.java).presence)
            .isEqualTo(JournalPresence.ABSENT)
        val wrong = DataValue.create(TmJournalPresenceAction.Data("wrong", old))
        assertThat(
            actions.execute(TmJournalPresenceAction.TYPE, wrong, 3)!!
                .getAsNotNull(TmJournalPresenceAction.Resp::class.java).presence
        ).isEqualTo(JournalPresence.UNKNOWN)
    }

    @Test
    fun `uncertain and failed queries retry after another minute`() {

        for (throwError in listOf(false, true)) {
            val storage = Storage(branch(old))
            var ready = false
            var queries = 0
            val sweep = sweep(storage) {
                queries++
                if (!ready && throwError) error("lookup timed out")
                if (ready) JournalPresence.ABSENT else JournalPresence.UNKNOWN
            }
            sweep.start(now)
            sweep.runIfDue(now.plusSeconds(60))
            assertThat(storage.rolledBack).isEmpty()
            ready = true
            sweep.runIfDue(now.plusSeconds(119))
            assertThat(queries).isEqualTo(1)
            sweep.runIfDue(now.plusSeconds(120))
            assertThat(storage.rolledBack).containsExactly(branch(old))
        }
    }

    @Test
    fun `unavailable and unsupported remote coordinator never queried as absent`() {

        for (available in listOf(false, true)) {
            var ready = false
            val requests = mutableListOf<String>()
            val client = remote { name, args ->
                requests.add(name)
                when (name) {
                    "isAppAvailable" -> available || ready
                    "isApiVersionSupported" -> if (ready) ApiVersionRes.SUPPORTED else ApiVersionRes.NOT_SUPPORTED
                    "getJournalPresence" -> {
                        assertThat(args!![0]).isEqualTo("coordinator")
                        assertThat(args[1]).isEqualTo(old)
                        JournalPresence.ABSENT
                    }
                    else -> error("Unexpected remote call $name")
                }
            }
            val manager = manager(NoopTwoPhaseCommitRepo, EcosTxnProps("coordinator"), "participant", client)
            val storage = Storage(branch(old))
            manager.getRecoveryManager().registerStorage(storage)
            val sweep = manager.createStartupOrphanRecovery()
            sweep.start(now)
            sweep.runIfDue(now.plusSeconds(60))
            assertThat(storage.rolledBack).isEmpty()
            assertThat(requests).doesNotContain("getJournalPresence")
            assertThat(storage.scans).isZero()
            ready = true
            sweep.runIfDue(now.plusSeconds(120))
            assertThat(storage.rolledBack).containsExactly(branch(old))
        }
    }

    @Test
    fun `journal read errors do not become absence`() {

        val repo = Journal()
        repo.fail = true
        val manager = manager(repo)
        val storage = Storage(branch(old))
        manager.getRecoveryManager().registerStorage(storage)
        val sweep = manager.createStartupOrphanRecovery()
        sweep.start(now)
        sweep.runIfDue(now.plusSeconds(60))
        assertThat(storage.rolledBack).isEmpty()
        repo.fail = false
        sweep.runIfDue(now.plusSeconds(120))
        assertThat(storage.rolledBack).containsExactly(branch(old))
    }

    @Test
    fun `enumeration failure completes no branch and retries complete source view`() {

        val recovery = RecoveryManagerImpl()
        val a = Storage(branch(old, "a"))
        val b = Storage(branch(old, "b"))
        b.failScan = true
        recovery.registerStorage(a)
        recovery.registerStorage(b)
        var queries = 0
        val sweep = StartupOrphanRecovery(recovery) {
            queries++
            JournalPresence.ABSENT
        }
        sweep.start(now)
        sweep.runIfDue(now.plusSeconds(60))
        assertThat(a.rolledBack).isEmpty()
        assertThat(queries).isZero()
        b.failScan = false
        sweep.runIfDue(now.plusSeconds(120))
        assertThat(a.rolledBack).containsExactly(branch(old, "a"))
        assertThat(b.rolledBack).containsExactly(branch(old, "b"))
    }

    @Test
    fun `partial rollback retry rechecks journal and does not repeat completed branch`() {

        val storage = Storage(branch(old, "a"), branch(old, "b"))
        storage.failRollback = branch(old, "b")
        var presence = JournalPresence.ABSENT
        val sweep = sweep(storage) { presence }
        sweep.start(now)
        sweep.runIfDue(now.plusSeconds(60))
        assertThat(storage.rolledBack).containsExactly(branch(old, "a"))
        presence = JournalPresence.PRESENT
        storage.failRollback = null
        sweep.runIfDue(now.plusSeconds(120))
        assertThat(storage.rolledBack).containsExactly(branch(old, "a"))
        assertThat(storage.prepared).containsExactly(branch(old, "b"))
    }

    @Test
    fun `known committed outcome prohibits orphan rollback`() {

        val recovery = RecoveryManagerImpl()
        val storage = Storage(branch(old))
        recovery.registerStorage(storage)
        recovery.rememberCompleted(old, emptyList(), true)
        var queries = 0
        val sweep = StartupOrphanRecovery(recovery) {
            queries++
            JournalPresence.ABSENT
        }
        sweep.start(now)
        sweep.runIfDue(now.plusSeconds(60))
        assertThat(storage.rolledBack).isEmpty()
        assertThat(queries).isZero()
    }

    @Test
    fun `same xid visible through multiple sources visits both`() {

        val recovery = RecoveryManagerImpl()
        val a = Storage(branch(old))
        val b = Storage(branch(old))
        recovery.registerStorage(a)
        recovery.registerStorage(b)
        val sweep = StartupOrphanRecovery(recovery) { JournalPresence.ABSENT }
        sweep.start(now)
        sweep.runIfDue(now.plusSeconds(60))
        assertThat(a.rolledBack).containsExactly(branch(old))
        assertThat(b.rolledBack).containsExactly(branch(old))
    }

    @Test
    fun `live transaction is not an orphan even when journal is absent`() {

        val repo = Journal()
        val manager = manager(repo)
        val transaction = TransactionImpl(old, "coordinator", false, EcosMicrometerContext.NOOP)
        transaction.start()
        manager.transactionsById[old] = TransactionManagerImpl.TransactionInfo(transaction)
        val storage = Storage(branch(old))
        manager.getRecoveryManager().registerStorage(storage)
        val sweep = manager.createStartupOrphanRecovery()
        sweep.start(now)
        sweep.runIfDue(now.plusSeconds(60))
        assertThat(storage.rolledBack).isEmpty()
        assertThat(repo.reads).isZero()
    }

    @Test
    fun `web client negotiates capability and reads explicit response from real remote action`() {

        val api = EcosWebAppApiMock("participant")
        var version = 2
        var requests = 0
        var missingResponse = false
        val server = TxnManagerRemoteActions(manager(Journal()))
        val webClient = object : EcosWebClientApi by api.getWebClientApi() {
            override fun getApiVersion(targetApp: String, path: String, maxVersion: Int): Int {
                return version
            }
        }
        api.webClientExecuteImpl = { app, path, bytes ->
            requests++
            assertThat(app).isEqualTo("coordinator")
            assertThat(path).isEqualTo("/txn")
            val request = Json.mapper.readNotNull(bytes as ByteArray, TmJournalPresenceAction.Data::class.java)
            assertThat(request.coordinatorApp).isEqualTo("coordinator")
            assertThat(request.txnId).isEqualTo(old)
            if (missingResponse) {
                emptyMap<String, String>()
            } else {
                server.execute(TmJournalPresenceAction.TYPE, DataValue.create(request), version)!!
            }
        }
        val client = TxnManagerWebApiClient(webClient, api.getRemoteWebAppsApi())
        assertThat(client.getJournalPresence("coordinator", old)).isEqualTo(JournalPresence.UNKNOWN)
        assertThat(requests).isZero()
        version = 3
        assertThat(client.getJournalPresence("coordinator", old)).isEqualTo(JournalPresence.ABSENT)
        assertThat(requests).isEqualTo(1)
        missingResponse = true
        assertThat(client.getJournalPresence("coordinator", old)).isEqualTo(JournalPresence.UNKNOWN)
    }

    @Test
    fun `partial orphan rollback prevents a later global commit of newly visible siblings`() {

        val recovery = RecoveryManagerImpl()
        val storage = Storage(branch(old, "a"), branch(old, "b"))
        storage.failRollback = branch(old, "b")
        recovery.registerStorage(storage)
        val sweep = StartupOrphanRecovery(recovery) { JournalPresence.ABSENT }
        sweep.start(now)
        sweep.runIfDue(now.plusSeconds(60))
        assertThat(storage.rolledBack).containsExactly(branch(old, "a"))
        storage.prepared.add(branch(old, "late"))
        assertThatThrownBy { recovery.commitPrepared(listOf(EcosXid.create(old, ByteArray(0)))) }
            .hasMessageContaining("Conflicting recovery decision")
        assertThat(storage.prepared).containsExactly(branch(old, "b"), branch(old, "late"))
        storage.failRollback = null
        sweep.runIfDue(now.plusSeconds(120))
        assertThat(storage.rolledBack).containsExactly(branch(old, "a"), branch(old, "b"), branch(old, "late"))
    }

    @Test
    fun `known commit of an already completed sibling prevents startup rollback of remainder`() {

        val recovery = RecoveryManagerImpl()
        val storage = Storage(branch(old, "a"), branch(old, "b"))
        storage.allowCommit = true
        storage.failCommitAfter = 1
        recovery.registerStorage(storage)
        assertThatThrownBy { recovery.commitPrepared(listOf(branch(old, "a"), branch(old, "b"))) }
            .hasMessage("Commit failed")
        assertThat(storage.committed).hasSize(1)
        val remaining = listOf(branch(old, "a"), branch(old, "b")).filter { it !in storage.committed }
        assertThat(storage.prepared).containsExactlyElementsOf(remaining)
        var queries = 0
        val sweep = StartupOrphanRecovery(recovery) {
            queries++
            JournalPresence.ABSENT
        }
        sweep.start(now)
        sweep.runIfDue(now.plusSeconds(60))
        assertThat(storage.rolledBack).isEmpty()
        assertThat(storage.prepared).containsExactlyElementsOf(remaining)
        assertThat(queries).isZero()
    }

    @Test
    fun `slow startup journal lookup does not block unrelated completion receipts`() {

        val recovery = RecoveryManagerImpl()
        recovery.registerStorage(Storage(branch(old)))
        val entered = java.util.concurrent.CountDownLatch(1)
        val release = java.util.concurrent.CountDownLatch(1)
        val sweep = StartupOrphanRecovery(recovery) {
            entered.countDown()
            check(release.await(10, java.util.concurrent.TimeUnit.SECONDS))
            JournalPresence.ABSENT
        }
        sweep.start(now)
        val sweepFuture = java.util.concurrent.CompletableFuture.runAsync {
            sweep.runIfDue(now.plusSeconds(60))
        }
        var receiptFuture: java.util.concurrent.CompletableFuture<Void>? = null
        try {
            check(entered.await(5, java.util.concurrent.TimeUnit.SECONDS))
            receiptFuture = java.util.concurrent.CompletableFuture.runAsync {
                val other = TxnId.create("other", "instance", now)
                recovery.rememberCompleted(other, emptyList(), true)
            }
            receiptFuture.get(1, java.util.concurrent.TimeUnit.SECONDS)
        } finally {
            release.countDown()
            sweepFuture.get(5, java.util.concurrent.TimeUnit.SECONDS)
            receiptFuture?.get(5, java.util.concurrent.TimeUnit.SECONDS)
        }
    }

    @Test
    fun `blank coordinator configuration cannot assert absence for a foreign journal`() {

        val foreignJournal = Journal(
            RecoveryData(old, TxnCommitData.EMPTY, TwoPhaseCommitStatus.COMMITTING, emptySet(), "origin:old", emptyMap())
        )
        manager(foreignJournal, EcosTxnProps(), "origin")
        val localJournal = Journal()
        val local = manager(localJournal, EcosTxnProps())
        val storage = Storage(branch(old))
        local.getRecoveryManager().registerStorage(storage)
        val sweep = local.createStartupOrphanRecovery()
        sweep.start(now)
        sweep.runIfDue(now.plusSeconds(60))
        assertThat(storage.rolledBack).isEmpty()
        assertThat(localJournal.reads).isZero()
        assertThat(storage.scans).isZero()
        sweep.runIfDue(now.plusSeconds(120))
        assertThat(storage.scans).isZero()
        assertThat(foreignJournal.data!!.status).isEqualTo(TwoPhaseCommitStatus.COMMITTING)
        val ownOld = TxnId.create("coordinator", "previous-instance", old.created)
        assertThat(local.getJournalPresence("coordinator", ownOld)).isEqualTo(JournalPresence.UNKNOWN)
    }

    @Test
    fun `commit receipt arriving during lookup prevents rollback of the remaining sibling`() {

        val recovery = RecoveryManagerImpl()
        val a = branch(old, "a")
        val b = branch(old, "b")
        val storage = Storage(a, b)
        storage.allowCommit = true
        storage.failCommitAfter = 1
        recovery.registerStorage(storage)
        val sweep = StartupOrphanRecovery(recovery) {
            assertThatThrownBy { recovery.commitPrepared(listOf(a, b)) }.hasMessage("Commit failed")
            JournalPresence.ABSENT
        }
        sweep.start(now)
        sweep.runIfDue(now.plusSeconds(60))
        assertThat(storage.committed).hasSize(1)
        val remaining = listOf(a, b).filter { it !in storage.committed }
        assertThat(storage.prepared).containsExactlyElementsOf(remaining)
        assertThat(storage.rolledBack).isEmpty()
        assertThat(storage.rollbackAttempts).isZero()
    }

    @Test
    fun `live local context appearing during remote lookup prevents startup rollback`() {

        lateinit var local: TransactionManagerImpl
        val client = remote { name, _ ->
            when (name) {
                "isAppAvailable" -> true
                "isApiVersionSupported" -> ApiVersionRes.SUPPORTED
                "getJournalPresence" -> {
                    val transaction = TransactionImpl(old, "participant", false, EcosMicrometerContext.NOOP)
                    transaction.start()
                    local.transactionsById[old] = TransactionManagerImpl.TransactionInfo(transaction)
                    JournalPresence.ABSENT
                }
                else -> error("Unexpected remote call $name")
            }
        }
        local = manager(NoopTwoPhaseCommitRepo, EcosTxnProps("coordinator"), "participant", client)
        val storage = Storage(branch(old))
        local.getRecoveryManager().registerStorage(storage)
        val sweep = local.createStartupOrphanRecovery()
        sweep.start(now)
        sweep.runIfDue(now.plusSeconds(60))
        assertThat(local.transactionsById).containsKey(old)
        assertThat(storage.rolledBack).isEmpty()
        assertThat(storage.rollbackAttempts).isZero()
    }

    @Test
    fun `ordinary rollback during journal lookup is not repeated from a stale snapshot`() {

        val recovery = RecoveryManagerImpl()
        val storage = Storage(branch(old))
        recovery.registerStorage(storage)
        val sweep = StartupOrphanRecovery(recovery) {
            recovery.rollbackPrepared(listOf(branch(old)))
            JournalPresence.ABSENT
        }
        sweep.start(now)
        sweep.runIfDue(now.plusSeconds(60))
        val completedScans = storage.scans
        sweep.runIfDue(now.plusSeconds(120))
        assertThat(storage.scans).isEqualTo(completedScans)
        assertThat(storage.rollbackAttempts).isEqualTo(1)
        assertThat(storage.rolledBack).containsExactly(branch(old))
    }

    @Test
    fun `failed fresh branch enumeration after lookup never permits rollback`() {

        val recovery = RecoveryManagerImpl()
        val storage = Storage(branch(old))
        recovery.registerStorage(storage)
        var failRefresh = true
        val sweep = StartupOrphanRecovery(recovery) {
            storage.failScan = failRefresh
            JournalPresence.ABSENT
        }
        sweep.start(now)
        sweep.runIfDue(now.plusSeconds(60))
        assertThat(storage.rolledBack).isEmpty()
        storage.failScan = false
        failRefresh = false
        sweep.runIfDue(now.plusSeconds(120))
        assertThat(storage.rolledBack).containsExactly(branch(old))
    }

    @Test
    fun `already resolved branch permits one retry and then finishes`() {

        val recovery = RecoveryManagerImpl()
        val storage = Storage(branch(old))
        var loserCalls = 0
        recovery.registerStorage(object : RecoverableStorage by storage {
            override fun rollbackPrepared(xid: EcosXid) {

                loserCalls++
                storage.rollbackPrepared(xid)
                throw javax.transaction.xa.XAException(javax.transaction.xa.XAException.XAER_NOTA)
            }
        })
        val sweep = StartupOrphanRecovery(recovery) { JournalPresence.ABSENT }
        sweep.start(now)
        sweep.runIfDue(now.plusSeconds(60))
        assertThat(loserCalls).isEqualTo(1)
        assertThat(storage.prepared).isEmpty()
        val scansAfterRace = storage.scans
        sweep.runIfDue(now.plusSeconds(120))
        val scansAfterRetry = storage.scans
        assertThat(scansAfterRetry).isEqualTo(scansAfterRace + 1)
        sweep.runIfDue(now.plusSeconds(180))
        assertThat(storage.scans).isEqualTo(scansAfterRetry)
        assertThat(loserCalls).isEqualTo(1)
    }

    private fun sweep(storage: Storage, lookup: (TxnId) -> JournalPresence): StartupOrphanRecovery {

        val recovery = RecoveryManagerImpl()
        recovery.registerStorage(storage)
        return StartupOrphanRecovery(recovery, journalPresence = lookup)
    }

    private fun branch(id: TxnId, name: String = "branch"): EcosXid {
        return EcosXid.create(id, name.toByteArray())
    }

    private fun manager(
        repo: TwoPhaseCommitRepo,
        props: EcosTxnProps = EcosTxnProps("coordinator"),
        appName: String = "coordinator",
        client: TxnManagerRemoteApiClient? = null
    ): TransactionManagerImpl {

        val manager = TransactionManagerImpl()
        manager.init(EcosWebAppApiMock(appName), props, twoPhaseCommitRepo = repo, remoteClient = client)
        managers.add(manager)
        return manager
    }

    private fun remote(invoke: (String, Array<out Any?>?) -> Any?): TxnManagerRemoteApiClient {

        return Proxy.newProxyInstance(
            javaClass.classLoader,
            arrayOf(TxnManagerRemoteApiClient::class.java)
        ) { _, method, args -> invoke(method.name, args) } as TxnManagerRemoteApiClient
    }

    private class Journal(var data: RecoveryData? = null) :
        AuthoritativeTwoPhaseCommitRepo, TwoPhaseCommitRepo by NoopTwoPhaseCommitRepo {

        var reads = 0
        var fail = false

        override fun getRecoveryData(txnId: TxnId): RecoveryData? {

            reads++
            if (fail) error("Journal unavailable")
            return data
        }
    }

    private class Storage(vararg xids: EcosXid) : RecoverableStorage {

        val prepared = xids.toMutableList()
        val rolledBack = mutableListOf<EcosXid>()
        val committed = mutableListOf<EcosXid>()
        var allowCommit = false
        var failCommitAfter: Int? = null
        var scans = 0
        var rollbackAttempts = 0
        var failScan = false
        var failRollback: EcosXid? = null

        override fun getPreparedXids(): List<EcosXid> {

            scans++
            if (failScan) error("Enumeration unavailable")
            return prepared.toList()
        }

        override fun commitPrepared(xid: EcosXid) {
            check(allowCommit) { "Startup sweep must never commit" }
            if (committed.size == failCommitAfter) error("Commit failed")
            check(prepared.remove(xid))
            committed.add(xid)
        }

        override fun rollbackPrepared(xid: EcosXid) {

            rollbackAttempts++
            if (xid == failRollback) error("Rollback failed")
            check(prepared.remove(xid))
            rolledBack.add(xid)
        }
    }
}
