package ru.citeck.ecos.txn.lib

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import ru.citeck.ecos.micrometer.EcosMicrometerContext
import ru.citeck.ecos.test.commons.EcosWebAppApiMock
import ru.citeck.ecos.txn.lib.action.TxnActionRef
import ru.citeck.ecos.txn.lib.action.TxnActionType
import ru.citeck.ecos.txn.lib.commit.RecoveryData
import ru.citeck.ecos.txn.lib.commit.TxnCommitData
import ru.citeck.ecos.txn.lib.commit.repo.NoopTwoPhaseCommitRepo
import ru.citeck.ecos.txn.lib.commit.repo.TwoPhaseCommitRepo
import ru.citeck.ecos.txn.lib.commit.repo.TwoPhaseCommitStatus
import ru.citeck.ecos.txn.lib.manager.EcosTxnProps
import ru.citeck.ecos.txn.lib.manager.TransactionManagerImpl
import ru.citeck.ecos.txn.lib.manager.TransactionPolicy
import ru.citeck.ecos.txn.lib.manager.api.client.ApiVersionRes
import ru.citeck.ecos.txn.lib.manager.api.client.TxnManagerRemoteApiClient
import ru.citeck.ecos.txn.lib.manager.recovery.RecoverableStorage
import ru.citeck.ecos.txn.lib.resource.CommitPrepareStatus
import ru.citeck.ecos.txn.lib.resource.TransactionResource
import ru.citeck.ecos.txn.lib.support.RoutingRemoteApiClient
import ru.citeck.ecos.txn.lib.transaction.TransactionImpl
import ru.citeck.ecos.txn.lib.transaction.TransactionStatus
import ru.citeck.ecos.txn.lib.transaction.TransactionSynchronization
import ru.citeck.ecos.txn.lib.transaction.TxnId
import ru.citeck.ecos.txn.lib.transaction.ctx.TxnManagerContext
import ru.citeck.ecos.txn.lib.transaction.xid.EcosXid
import java.util.concurrent.CopyOnWriteArrayList

class TxnRecoveryFlowTest {

    private val managers = mutableListOf<TransactionManagerImpl>()

    @AfterEach
    fun tearDown() {
        managers.forEach { it.shutdown() }
    }

    @Test
    fun `synchronization resources are journaled before any prepare and callback runs once`() {
        val repo = InMemoryTwoPhaseCommitRepo()
        val events = CopyOnWriteArrayList<String>()
        val registry = mutableMapOf<String, TransactionManagerImpl>()
        val app = createApp("app-a", RoutingRemoteApiClient(registry), repo = repo)
        registry["app-a"] = app.manager
        TxnContext.setManager(app.manager)
        var calls = 0
        lateinit var lateXid: EcosXid

        TxnContext.doInTxn {
            val txn = TxnContext.getTxn()
            txn.getOrAddRes("initial") { _, id -> TestResource("initial", id, events) }
            txn.registerSync(object : TransactionSynchronization {
                override fun beforeCompletion() {
                    calls++
                    lateXid = txn.getOrAddRes("late") { _, id -> TestResource("late", id, events) }.getXid()
                    // Actions discovered while flushing must also finish before the snapshot.
                    txn.addAction(TxnActionType.BEFORE_COMMIT, 0f) {
                        txn.getOrAddRes("nested") { _, id -> TestResource("nested", id, events) }
                    }
                }
            })
        }

        assertThat(calls).isEqualTo(1)
        assertThat(repo.preparedData!!.apps["app-a"]).hasSize(3).contains(lateXid)
        assertThat(events).contains("late:prepareCommit", "nested:prepareCommit", "late:commitPrepared")
        assertThat(events).doesNotContain("initial:onePhaseCommit")
    }

    @Test
    fun `dynamically ordered writes precede the once-only synchronization flush`() {
        val registry = mutableMapOf<String, TransactionManagerImpl>()
        val app = createApp("app-a", RoutingRemoteApiClient(registry))
        registry["app-a"] = app.manager
        TxnContext.setManager(app.manager)
        var pending = 0
        var flushed = -1
        var committed = -1
        var callbacks = 0

        TxnContext.doInTxn {
            val txn = TxnContext.getTxn()
            val resource = TestResource("managed", txn.getId(), mutableListOf())
            txn.getOrAddRes("managed") { _, _ ->
                object : TransactionResource by resource {
                    override fun onePhaseCommit() {
                        committed = flushed
                        resource.onePhaseCommit()
                    }
                }
            }
            txn.registerSync(object : TransactionSynchronization {
                override fun beforeCompletion() {
                    callbacks++
                    flushed = pending
                }
            })
            // A large pre-existing batch must not consume the dynamic-expansion guard.
            repeat(1100) {
                txn.addAction(TxnActionType.BEFORE_COMMIT, 0f) { pending++ }
            }
            txn.addAction(TxnActionType.BEFORE_COMMIT, 0f) {
                txn.addAction(TxnActionType.BEFORE_COMMIT, 0f) { pending = 1234 }
            }
        }

        assertThat(callbacks).isEqualTo(1)
        assertThat(pending).isEqualTo(1234)
        assertThat(committed).isEqualTo(1234)
    }

    @Test
    fun `synchronization registered in nested read-only scope flushes before writable snapshot`() {
        val repo = InMemoryTwoPhaseCommitRepo()
        val events = mutableListOf<String>()
        val registry = mutableMapOf<String, TransactionManagerImpl>()
        val app = createApp("app-a", RoutingRemoteApiClient(registry), repo = repo)
        registry["app-a"] = app.manager
        TxnContext.setManager(app.manager)

        TxnContext.doInTxn {
            val txn = TxnContext.getTxn()
            txn.getOrAddRes("initial") { _, id -> TestResource("initial", id, events) }
            TxnContext.doInTxn(readOnly = true) {
                txn.registerSync(object : TransactionSynchronization {
                    override fun beforeCompletion() {
                        txn.getOrAddRes("late") { _, id -> TestResource("late", id, events) }
                    }
                })
            }
        }

        assertThat(repo.preparedData!!.apps["app-a"]).hasSize(2)
        assertThat(events).contains("late:prepareCommit", "late:commitPrepared")
    }

    @Test
    fun `completed recovery branch after restart acknowledges durable decision for commit and rollback`() {
        for (status in listOf(TwoPhaseCommitStatus.COMMITTING, TwoPhaseCommitStatus.ROLLING_BACK)) {
            val repo = InMemoryTwoPhaseCommitRepo()
            val registry = mutableMapOf<String, TransactionManagerImpl>()
            val client = RoutingRemoteApiClient(registry)
            val coordinator = createApp("app-a", client, repo = repo)
            val participant = createApp("app-b", client)
            registry["app-a"] = coordinator.manager
            registry["app-b"] = participant.manager
            val txnId = TxnId.create("app-a", "lost-instance")
            val xid = EcosXid.create(txnId, "missing".toByteArray())
            repo.ownRecoveryData = RecoveryData(
                txnId,
                TxnCommitData(mapOf("app-b" to setOf(xid)), emptyMap()),
                status,
                setOf("app-b"),
                "app-a",
                emptyMap()
            )
            participant.manager.getRecoveryManager().registerStorage(object : RecoverableStorage {
                override fun getPreparedXids(): List<EcosXid> {
                    return emptyList()
                }

                override fun commitPrepared(xid: EcosXid) {
                    error("No resource operation is needed for an already completed branch")
                }

                override fun rollbackPrepared(xid: EcosXid) {
                    error("No resource operation is needed for an already completed branch")
                }
            })

            coordinator.manager.commitCoordinator.runTxnRecovering()

            assertThat(repo.ownRecoveryData).isNull()
            val acknowledgement = if (status == TwoPhaseCommitStatus.COMMITTING) "afterCommit" else "afterRollback"
            assertThat(repo.calls).contains("$acknowledgement:$txnId")
        }
    }

    @Test
    fun `stateless participant after restart acknowledges its empty snapshot`() {

        for (status in listOf(TwoPhaseCommitStatus.COMMITTING, TwoPhaseCommitStatus.ROLLING_BACK)) {
            val repo = InMemoryTwoPhaseCommitRepo()
            val registry = mutableMapOf<String, TransactionManagerImpl>()
            val client = RoutingRemoteApiClient(registry)
            val coordinator = createApp("app-a", client, repo = repo)
            registry["app-a"] = coordinator.manager
            registry["app-b"] = createApp("app-b", client).manager
            val txnId = TxnId.create("app-a", "lost-instance")
            repo.ownRecoveryData = RecoveryData(
                txnId,
                TxnCommitData(mapOf("app-b" to emptySet()), emptyMap()),
                status,
                setOf("app-b"),
                "app-a",
                emptyMap()
            )

            coordinator.manager.commitCoordinator.runTxnRecovering()

            assertThat(repo.ownRecoveryData).isNull()
            val acknowledgement = if (status == TwoPhaseCommitStatus.COMMITTING) "afterCommit" else "afterRollback"
            assertThat(repo.calls).contains("$acknowledgement:$txnId")
        }
    }

    @Test
    fun `global recovery covers empty snapshots and refuses legacy participants`() {
        for (commit in listOf(true, false)) {
            for (emptySnapshot in listOf(true, false)) {
                for (legacyParticipant in listOf(true, false)) {
                    val repo = InMemoryTwoPhaseCommitRepo()
                    val registry = mutableMapOf<String, TransactionManagerImpl>()
                    val delegate = RoutingRemoteApiClient(registry)
                    val client = object : TxnManagerRemoteApiClient by delegate {
                        override fun isApiVersionSupported(app: String, version: Int): ApiVersionRes {
                            if (app == "app-b" && legacyParticipant && version > 2) {
                                return ApiVersionRes.NOT_SUPPORTED
                            }
                            return delegate.isApiVersionSupported(app, version)
                        }
                    }
                    val coordinator = createApp("app-a", client, repo = repo)
                    val participant = createApp("app-b", client)
                    registry["app-a"] = coordinator.manager
                    registry["app-b"] = participant.manager
                    val txnId = TxnId.create("app-a", "lost-instance")
                    val xid = EcosXid.create(txnId, "late".toByteArray())
                    val prepared = mutableSetOf(xid)
                    val outcomes = mutableListOf<Boolean>()
                    participant.manager.getRecoveryManager().registerStorage(object : RecoverableStorage {
                        override fun getPreparedXids(): List<EcosXid> {
                            return prepared.toList()
                        }

                        override fun commitPrepared(xid: EcosXid) {
                            check(prepared.remove(xid))
                            outcomes.add(true)
                        }

                        override fun rollbackPrepared(xid: EcosXid) {
                            check(prepared.remove(xid))
                            outcomes.add(false)
                        }
                    })
                    val journalXids = if (emptySnapshot) emptySet() else setOf(xid)
                    repo.ownRecoveryData = RecoveryData(
                        txnId,
                        TxnCommitData(mapOf("app-b" to journalXids), emptyMap()),
                        if (commit) TwoPhaseCommitStatus.COMMITTING else TwoPhaseCommitStatus.PREPARING,
                        setOf("app-b"),
                        "app-a",
                        emptyMap()
                    )

                    coordinator.manager.commitCoordinator.runTxnRecovering()

                    if (legacyParticipant) {
                        assertThat(repo.ownRecoveryData).isNotNull()
                        assertThat(prepared).containsExactly(xid)
                        assertThat(outcomes).isEmpty()
                    } else {
                        assertThat(prepared).isEmpty()
                        assertThat(outcomes).containsExactly(commit)
                        assertThat(repo.ownRecoveryData).isNull()
                    }
                }
            }
        }
    }

    @Test
    fun `lost prepare response leaves every branch recoverable from the original journal`() {
        val repo = InMemoryTwoPhaseCommitRepo()
        val registry = mutableMapOf<String, TransactionManagerImpl>()
        val delegate = RoutingRemoteApiClient(registry)
        var participantLost = false
        val client = object : TxnManagerRemoteApiClient by delegate {
            override fun prepareCommit(app: String, txnId: TxnId): List<EcosXid> {
                val result = delegate.prepareCommit(app, txnId)
                if (app == "app-a") {
                    participantLost = true
                    error("Prepare response lost")
                }
                return result
            }

            override fun rollback(app: String, txnId: TxnId, cause: Throwable?) {
                check(!participantLost) { "Participant unavailable" }
                delegate.rollback(app, txnId, cause)
            }

            override fun disposeTxn(app: String, txnId: TxnId) {
                check(!participantLost) { "Participant unavailable" }
                delegate.disposeTxn(app, txnId)
            }
        }
        val root = createApp("app-a", client, EcosTxnProps("emodel"))
        val coordinator = createApp("emodel", client, repo = repo)
        registry["app-a"] = root.manager
        registry["emodel"] = coordinator.manager
        TxnContext.setManager(root.manager)
        val prepared = mutableSetOf<EcosXid>()
        val events = mutableListOf<String>()
        var callbacks = 0

        fun resource(name: String, txnId: TxnId): TransactionResource {
            val base = TestResource(name, txnId, events)
            return object : TransactionResource by base {
                override fun prepareCommit(): CommitPrepareStatus {
                    val result = base.prepareCommit()
                    prepared.add(base.getXid())
                    return result
                }

                override fun rollback() {
                    check(!participantLost) { "Storage unavailable" }
                    prepared.remove(base.getXid())
                }
            }
        }

        assertThatThrownBy {
            TxnContext.doInTxn {
                val txn = TxnContext.getTxn()
                txn.getOrAddRes("initial") { _, id -> resource("initial", id) }
                txn.registerSync(object : TransactionSynchronization {
                    override fun beforeCompletion() {
                        callbacks++
                        txn.getOrAddRes("late") { _, id -> resource("late", id) }
                    }
                })
            }
        }.hasMessage("Prepare response lost")

        assertThat(callbacks).isEqualTo(1)
        val snapshot = repo.preparedData!!
        assertThat(snapshot.apps["app-a"]).containsExactlyInAnyOrderElementsOf(prepared)
        assertThat(prepared).hasSize(2)
        val txnId = prepared.first().getTransactionId()
        // Discard all participant process state without disposing the durable branches.
        root.manager.transactionsById.clear()
        root.manager.getRecoveryManager().registerStorage(object : RecoverableStorage {
            override fun getPreparedXids(): List<EcosXid> {
                return prepared.toList()
            }

            override fun commitPrepared(xid: EcosXid) {
                error("A failed prepare must be rolled back")
            }

            override fun rollbackPrepared(xid: EcosXid) {
                prepared.remove(xid)
            }
        })
        repo.ownRecoveryData = RecoveryData(
            txnId,
            snapshot,
            TwoPhaseCommitStatus.PREPARING,
            setOf("app-a"),
            "emodel",
            emptyMap()
        )

        coordinator.manager.commitCoordinator.runTxnRecovering()

        assertThat(prepared).isEmpty()
        assertThat(repo.ownRecoveryData).isNull()
    }

    @Test
    fun `synchronization can discover a remote participant with another synchronization`() {
        val repo = InMemoryTwoPhaseCommitRepo()
        val registry = mutableMapOf<String, TransactionManagerImpl>()
        val delegate = RoutingRemoteApiClient(registry)
        lateinit var remoteContext: TxnManagerContext
        val client = object : TxnManagerRemoteApiClient by delegate {
            override fun executeTxnAction(app: String, txnId: TxnId, actionId: Int) {
                // Model WebAPI propagation of the current transaction and response metadata.
                registry.getValue(app).doInExtTxn(
                    txnId,
                    remoteContext,
                    TransactionPolicy.REQUIRED,
                    false
                ) {
                    delegate.executeTxnAction(app, txnId, actionId)
                }
            }
        }
        val root = createApp("app-a", client, repo = repo)
        val participant = createApp("app-b", client)
        registry["app-a"] = root.manager
        registry["app-b"] = participant.manager
        TxnContext.setManager(root.manager)
        val events = mutableListOf<String>()
        var callbacks = 0

        TxnContext.doInTxn {
            val rootTxn = TxnContext.getTxn()
            remoteContext = object : TxnManagerContext {
                override fun registerAction(type: TxnActionType, actionRef: TxnActionRef) {
                    rootTxn.addAction(type, actionRef)
                }

                override fun registerXids(appName: String, xids: Collection<EcosXid>) {
                    rootTxn.registerXids(appName, xids)
                }
            }
            rootTxn.getOrAddRes("root") { _, id -> TestResource("root", id, events) }
            rootTxn.registerSync(object : TransactionSynchronization {
                override fun beforeCompletion() {
                    callbacks++
                    participant.manager.doInExtTxn(
                        rootTxn.getId(),
                        remoteContext,
                        TransactionPolicy.REQUIRED,
                        false
                    ) {
                        val remoteTxn = participant.manager.getManagedTransaction(rootTxn.getId())
                        remoteTxn.getOrAddRes("remote") { _, id -> TestResource("remote", id, events) }
                        remoteTxn.registerSync(object : TransactionSynchronization {
                            override fun beforeCompletion() {
                                callbacks++
                                remoteTxn.getOrAddRes("remote-late") { _, id ->
                                    TestResource("remote-late", id, events)
                                }
                            }
                        })
                    }
                }
            })
        }

        assertThat(callbacks).isEqualTo(2)
        assertThat(repo.preparedData!!.apps["app-b"]).hasSize(2)
        assertThat(events).contains("remote-late:prepareCommit", "remote-late:commitPrepared")
    }

    @Test
    fun `2PC with repo records prepare and commit lifecycle`() {
        val repo = InMemoryTwoPhaseCommitRepo()
        val events = CopyOnWriteArrayList<String>()

        val registryMap = mutableMapOf<String, TransactionManagerImpl>()
        val routingClient = RoutingRemoteApiClient(registryMap)

        val appA = createApp("app-a", routingClient, repo = repo)
        val appB = createApp("app-b", routingClient)
        registryMap["app-a"] = appA.manager
        registryMap["app-b"] = appB.manager

        TxnContext.setManager(appA.manager)

        val txnId = TxnId.create("app-a", appA.appMock.getProperties().appInstanceId)
        val xidsA = setupAppTransaction(appA, txnId, events, "resA")
        val xidsB = setupAppTransaction(appB, txnId, events, "resB")

        val commitData = TxnCommitData(
            mapOf("app-a" to xidsA, "app-b" to xidsB),
            emptyMap()
        )

        appA.manager.coordinateCommit(txnId, commitData, 0)

        // Repo should have recorded the lifecycle
        assertThat(repo.calls).contains("beforePrepare:$txnId")
        assertThat(repo.calls).contains("beforeCommit:$txnId")
        assertThat(repo.calls).contains("afterCommit:$txnId")
        // beforePrepare is before beforeCommit
        assertThat(repo.calls.indexOf("beforePrepare:$txnId"))
            .isLessThan(repo.calls.indexOf("beforeCommit:$txnId"))
    }

    @Test
    fun `prepare failure records rollback in repo`() {
        val repo = InMemoryTwoPhaseCommitRepo()
        val events = CopyOnWriteArrayList<String>()

        val registryMap = mutableMapOf<String, TransactionManagerImpl>()
        val routingClient = RoutingRemoteApiClient(registryMap)

        val appA = createApp("app-a", routingClient, repo = repo)
        val appB = createApp("app-b", routingClient)
        registryMap["app-a"] = appA.manager
        registryMap["app-b"] = appB.manager

        TxnContext.setManager(appA.manager)

        val txnId = TxnId.create("app-a", appA.appMock.getProperties().appInstanceId)
        val xidsA = setupAppTransaction(appA, txnId, events, "resA")
        val xidsB = setupAppTransaction(appB, txnId, events, "resB", throwOnPrepare = true)

        val commitData = TxnCommitData(
            mapOf("app-a" to xidsA, "app-b" to xidsB),
            emptyMap()
        )

        assertThatThrownBy {
            appA.manager.coordinateCommit(txnId, commitData, 0)
        }.hasMessageContaining("prepare failed")

        assertThat(repo.calls).contains("beforePrepare:$txnId")
        assertThat(repo.calls).contains("beforeRollback:$txnId")
        assertThat(repo.calls).doesNotContain("beforeCommit:$txnId")
    }

    @Test
    fun `commit failure triggers afterCommit with errors`() {
        val repo = InMemoryTwoPhaseCommitRepo()
        val events = CopyOnWriteArrayList<String>()

        val registryMap = mutableMapOf<String, TransactionManagerImpl>()
        val routingClient = RoutingRemoteApiClient(registryMap)

        val appA = createApp("app-a", routingClient, repo = repo)
        val appB = createApp("app-b", routingClient)
        registryMap["app-a"] = appA.manager
        registryMap["app-b"] = appB.manager

        TxnContext.setManager(appA.manager)

        val txnId = TxnId.create("app-a", appA.appMock.getProperties().appInstanceId)
        val xidsA = setupAppTransaction(appA, txnId, events, "resA")
        val xidsB = setupAppTransaction(appB, txnId, events, "resB", throwOnCommit = true)

        val commitData = TxnCommitData(
            mapOf("app-a" to xidsA, "app-b" to xidsB),
            emptyMap()
        )

        assertThatThrownBy {
            appA.manager.coordinateCommit(txnId, commitData, 0)
        }.hasMessageContaining("commit failed")

        assertThat(repo.calls).contains("beforePrepare:$txnId")
        assertThat(repo.calls).contains("beforeCommit:$txnId")
        assertThat(repo.calls).contains("afterCommit:$txnId")
        // afterCommit was called with errors
        assertThat(repo.commitErrors).containsKey(txnId)
        assertThat(repo.commitErrors[txnId]).isNotEmpty()
    }

    @Test
    fun `runTxnRecovering commits failed transactions`() {
        val repo = InMemoryTwoPhaseCommitRepo()
        val events = CopyOnWriteArrayList<String>()

        val registryMap = mutableMapOf<String, TransactionManagerImpl>()
        val routingClient = RoutingRemoteApiClient(registryMap)

        val appA = createApp("app-a", routingClient, repo = repo)
        val appB = createApp("app-b", routingClient)
        registryMap["app-a"] = appA.manager
        registryMap["app-b"] = appB.manager

        TxnContext.setManager(appA.manager)

        val txnId = TxnId.create("app-a", appA.appMock.getProperties().appInstanceId)
        val xidsA = setupAppTransaction(appA, txnId, events, "resA")
        val xidsB = setupAppTransaction(appB, txnId, events, "resB", throwOnCommit = true)

        val commitData = TxnCommitData(
            mapOf("app-a" to xidsA, "app-b" to xidsB),
            emptyMap()
        )

        assertThatThrownBy {
            appA.manager.coordinateCommit(txnId, commitData, 0)
        }.hasMessageContaining("commit failed")

        // Now set up the repo to return recovery data for the failed txn
        // The commit failed for app-b, so recovery should try to commit it
        repo.ownRecoveryData = RecoveryData(
            txnId = txnId,
            data = commitData,
            status = TwoPhaseCommitStatus.COMMITTING,
            appsToProcess = setOf("app-b"),
            ownerApp = "app-a",
            appRoutes = emptyMap()
        )

        // Re-setup app-b's transaction so recovery can commit it
        setupAppTransaction(appB, txnId, events, "resB2")
        // Prepare it so recovery-commit can succeed
        appB.manager.prepareCommitFromExtManager(txnId, true)

        // Run recovery
        appA.manager.commitCoordinator.runTxnRecovering()

        // Recovery should have committed and the afterCommit should be called
        assertThat(repo.calls.count { it == "afterCommit:$txnId" }).isGreaterThanOrEqualTo(2)
        assertThat(repo.ownRecoveryData).isNull()
        assertThat(events.count { it == "resB2:commitPrepared" }).isEqualTo(1)
        assertThat(appB.manager.getStatus(txnId)).isEqualTo(TransactionStatus.COMMITTED)
    }

    @Test
    fun `runTxnRecovering rolls back PREPARING transactions`() {
        val repo = InMemoryTwoPhaseCommitRepo()

        val registryMap = mutableMapOf<String, TransactionManagerImpl>()
        val routingClient = RoutingRemoteApiClient(registryMap)

        val appA = createApp("app-a", routingClient, repo = repo)
        val appB = createApp("app-b", routingClient)
        registryMap["app-a"] = appA.manager
        registryMap["app-b"] = appB.manager

        TxnContext.setManager(appA.manager)

        val txnId = TxnId.create("app-a", appA.appMock.getProperties().appInstanceId)
        val events = CopyOnWriteArrayList<String>()
        val xidsA = setupAppTransaction(appA, txnId, events, "resA")

        val commitData = TxnCommitData(
            mapOf("app-a" to xidsA),
            emptyMap()
        )

        // Remove from in-memory map to simulate a restart recovery scenario
        appA.manager.transactionsById.remove(txnId)
        val prepared = xidsA.toMutableSet()
        appA.manager.getRecoveryManager().registerStorage(object : RecoverableStorage {
            override fun getPreparedXids(): List<EcosXid> {
                return prepared.toList()
            }

            override fun commitPrepared(xid: EcosXid) {
                error("PREPARING recovery must not commit")
            }

            override fun rollbackPrepared(xid: EcosXid) {
                prepared.remove(xid)
            }
        })

        // Simulate a PREPARING state recovery scenario
        repo.ownRecoveryData = RecoveryData(
            txnId = txnId,
            data = commitData,
            status = TwoPhaseCommitStatus.PREPARING,
            appsToProcess = setOf("app-a"),
            ownerApp = "app-a",
            appRoutes = emptyMap()
        )

        appA.manager.commitCoordinator.runTxnRecovering()

        // PREPARING means rollback
        assertThat(repo.calls).contains("beforeRollback:$txnId")
        assertThat(repo.calls).contains("afterRollback:$txnId")
        assertThat(prepared).isEmpty()
        assertThat(repo.ownRecoveryData).isNull()
    }

    @Test
    fun `delegation keeps the recovery route and never rolls back after response loss`() {
        for (partialCommit in listOf(true, false)) {
            val repo = InMemoryTwoPhaseCommitRepo()
            val registry = mutableMapOf<String, TransactionManagerImpl>()
            val delegate = RoutingRemoteApiClient(registry)
            var rollbacks = 0
            val client = object : TxnManagerRemoteApiClient by delegate {
                override fun coordinateCommit(app: String, txnId: TxnId, data: TxnCommitData, txnLevel: Int) {
                    delegate.coordinateCommit(app, txnId, data, txnLevel)
                    error("Coordination response lost")
                }

                override fun rollback(app: String, txnId: TxnId, cause: Throwable?) {
                    rollbacks++
                    delegate.rollback(app, txnId, cause)
                }
            }
            val root = createApp("app-a", client, EcosTxnProps("emodel"))
            val coordinator = createApp("emodel", client, repo = repo)
            registry["app-a"] = root.manager
            registry["emodel"] = coordinator.manager
            TxnContext.setManager(root.manager)
            lateinit var txn: ru.citeck.ecos.txn.lib.transaction.ManagedTransaction
            val events = mutableListOf<String>()
            var failOnce = partialCommit
            assertThatThrownBy {
                TxnContext.doInTxn {
                    txn = TxnContext.getTxn() as ru.citeck.ecos.txn.lib.transaction.ManagedTransaction
                    txn.getOrAddRes("first") { _, id -> TestResource("first", id, events) }
                    txn.getOrAddRes("second") { _, id ->
                        val resource = TestResource("second", id, events)
                        object : TransactionResource by resource {
                            override fun commitPrepared() {
                                if (failOnce) {
                                    failOnce = false
                                    error("Transient commit failure")
                                }
                                resource.commitPrepared()
                            }
                        }
                    }
                }
            }.hasMessage(if (partialCommit) "Transient commit failure" else "Coordination response lost")

            assertThat(rollbacks).isZero()
            assertThat(events).doesNotContain("first:rollback", "second:rollback")
            if (partialCommit) {
                assertThat(root.manager.transactionsById[txn.getId()]!!.commitDelegatedToApp).isEqualTo("emodel")
                repo.ownRecoveryData = RecoveryData(
                    txn.getId(),
                    repo.preparedData!!,
                    TwoPhaseCommitStatus.COMMITTING,
                    setOf("app-a"),
                    "emodel",
                    mapOf("app-a" to root.appMock.getProperties().appInstanceId)
                )
                coordinator.manager.commitCoordinator.runTxnRecovering()
                assertThat(repo.ownRecoveryData).isNull()
            }
            assertThat(events.count { it == "first:commitPrepared" }).isEqualTo(1)
            assertThat(events.count { it == "second:commitPrepared" }).isEqualTo(1)
            assertThat(root.manager.getManagedTransactionOrNull(txn.getId())).isNull()
        }
    }

    @Test
    fun `remote-only commit failure recovers independently of the ACTIVE root context`() {
        val repo = InMemoryTwoPhaseCommitRepo()
        val registry = mutableMapOf<String, TransactionManagerImpl>()
        val delegate = RoutingRemoteApiClient(registry)
        var rollbacks = 0
        val client = object : TxnManagerRemoteApiClient by delegate {
            override fun rollback(app: String, txnId: TxnId, cause: Throwable?) {
                rollbacks++
                delegate.rollback(app, txnId, cause)
            }
        }
        val root = createApp("app-a", client, repo = repo)
        val participant = createApp("app-b", client)
        registry["app-a"] = root.manager
        registry["app-b"] = participant.manager
        TxnContext.setManager(root.manager)
        val events = mutableListOf<String>()
        lateinit var txnId: TxnId
        lateinit var xids: Set<EcosXid>
        var failOnce = true
        assertThatThrownBy {
            TxnContext.doInTxn {
                val rootTxn = TxnContext.getTxn()
                txnId = rootTxn.getId()
                val collected = setupAppTransaction(participant, txnId, events, "first").toMutableSet()
                val remoteTxn = participant.manager.getManagedTransaction(txnId)
                remoteTxn.doWithinTxn(ru.citeck.ecos.txn.lib.transaction.ctx.NoopTxnManagerContext, false) {
                    val second = remoteTxn.getOrAddRes("second") { _, id ->
                        val resource = TestResource("second", id, events)
                        object : TransactionResource by resource {
                            override fun commitPrepared() {
                                if (failOnce) {
                                    failOnce = false
                                    error("Transient commit failure")
                                }
                                resource.commitPrepared()
                            }
                        }
                    }
                    collected.add(second.getXid())
                }
                xids = collected
                rootTxn.registerXids("app-b", xids)
            }
        }.hasMessage("Transient commit failure")

        assertThat(rollbacks).isZero()
        assertThat(root.manager.getManagedTransaction(txnId).getStatus())
            .isEqualTo(ru.citeck.ecos.txn.lib.transaction.TransactionStatus.ACTIVE)
        root.manager.dispose(txnId)
        repo.ownRecoveryData = RecoveryData(
            txnId,
            repo.preparedData!!,
            TwoPhaseCommitStatus.COMMITTING,
            setOf("app-b"),
            "app-a",
            mapOf("app-b" to participant.appMock.getProperties().appInstanceId)
        )

        root.manager.commitCoordinator.runTxnRecovering()

        assertThat(repo.ownRecoveryData).isNull()
        assertThat(events).doesNotContain("first:rollback", "second:rollback")
        assertThat(events.count { it == "first:commitPrepared" }).isEqualTo(1)
        assertThat(events.count { it == "second:commitPrepared" }).isEqualTo(1)
        assertThat(participant.manager.getManagedTransactionOrNull(txnId)).isNull()
    }

    @Test
    fun `acknowledged recovery participants release contexts without losing retry receipts`() {
        val repo = InMemoryTwoPhaseCommitRepo()
        val journal = object : TwoPhaseCommitRepo by repo {
            override fun findOwnDataToRecover(exclusions: List<TxnId>): RecoveryData? {
                return repo.ownRecoveryData?.takeUnless { it.txnId in exclusions }
            }

            override fun afterCommit(txnId: TxnId, committedApps: Set<String>, errors: Map<String, Throwable>) {
                val state = repo.ownRecoveryData ?: return
                val remaining = state.appsToProcess - committedApps
                repo.ownRecoveryData = if (remaining.isEmpty()) null else state.copy(appsToProcess = remaining)
            }
        }
        val registry = mutableMapOf<String, TransactionManagerImpl>()
        val delegate = RoutingRemoteApiClient(registry)
        var unavailable = true
        val client = object : TxnManagerRemoteApiClient by delegate {
            override fun recoveryCommit(app: String, txnId: TxnId, xids: Set<EcosXid>) {
                check(!app.startsWith("app-b") || !unavailable) { "Participant temporarily unavailable" }
                delegate.recoveryCommit(app, txnId, xids)
            }
        }
        val coordinator = createApp("emodel", client, repo = journal)
        val first = createApp("app-a", client)
        val second = createApp("app-b", client)
        registry["emodel"] = coordinator.manager
        registry["app-a"] = first.manager
        registry["app-b"] = second.manager
        val txnId = TxnId.create("origin", "lost-instance")
        val events = mutableListOf<String>()
        val firstXids = setupAppTransaction(first, txnId, events, "first")
        val secondXids = setupAppTransaction(second, txnId, events, "second")
        first.manager.prepareCommitFromExtManager(txnId, true)
        second.manager.prepareCommitFromExtManager(txnId, true)
        repo.ownRecoveryData = RecoveryData(
            txnId,
            TxnCommitData(mapOf("app-a" to firstXids, "app-b" to secondXids), emptyMap()),
            TwoPhaseCommitStatus.COMMITTING,
            setOf("app-a", "app-b"),
            "emodel",
            mapOf(
                "app-a" to first.appMock.getProperties().appInstanceId,
                "app-b" to second.appMock.getProperties().appInstanceId
            )
        )

        coordinator.manager.commitCoordinator.runTxnRecovering()

        assertThat(repo.ownRecoveryData!!.appsToProcess).containsExactly("app-b")
        assertThat(first.manager.getManagedTransaction(txnId).isCompleted()).isTrue()
        // Exercise the same disposal path used by idle cleanup of completed participants.
        first.manager.dispose(txnId)
        first.manager.recoveryCommit(txnId, firstXids)
        assertThat(first.manager.getManagedTransactionOrNull(txnId)).isNull()
        unavailable = false

        coordinator.manager.commitCoordinator.runTxnRecovering()

        assertThat(repo.ownRecoveryData).isNull()
        assertThat(first.manager.getManagedTransactionOrNull(txnId)).isNull()
        assertThat(second.manager.getManagedTransactionOrNull(txnId)).isNull()
        assertThat(events.count { it == "first:commitPrepared" }).isEqualTo(1)
        assertThat(events.count { it == "second:commitPrepared" }).isEqualTo(1)
    }

    // ==================== Helpers ====================

    private data class AppSetup(
        val name: String,
        val manager: TransactionManagerImpl,
        val appMock: EcosWebAppApiMock
    )

    private fun createApp(
        appName: String,
        routingClient: TxnManagerRemoteApiClient,
        props: EcosTxnProps = EcosTxnProps(),
        repo: TwoPhaseCommitRepo = NoopTwoPhaseCommitRepo
    ): AppSetup {
        val appMock = EcosWebAppApiMock(appName)
        val manager = TransactionManagerImpl()
        manager.init(appMock, props, twoPhaseCommitRepo = repo, remoteClient = routingClient)
        managers.add(manager)
        return AppSetup(appName, manager, appMock)
    }

    private fun setupAppTransaction(
        app: AppSetup,
        txnId: TxnId,
        events: MutableList<String>,
        resourceName: String,
        throwOnPrepare: Boolean = false,
        throwOnCommit: Boolean = false
    ): Set<EcosXid> {
        val collectedXids = mutableSetOf<EcosXid>()
        val txn = TransactionImpl(
            txnId,
            app.name,
            false,
            EcosMicrometerContext.NOOP
        )
        val ctx = object : TxnManagerContext {
            override fun registerAction(type: TxnActionType, actionRef: TxnActionRef) {}
            override fun registerXids(appName: String, xids: Collection<EcosXid>) {
                collectedXids.addAll(xids)
            }
        }
        txn.start()
        app.manager.transactionsById[txnId] =
            TransactionManagerImpl.TransactionInfo(txn)
        txn.doWithinTxn(ctx, false) {
            txn.getOrAddRes(resourceName) { _, id ->
                TestResource(resourceName, id, events, throwOnPrepare = throwOnPrepare, throwOnCommit = throwOnCommit)
            }
        }
        return collectedXids
    }

    private class TestResource(
        private val name: String,
        private val txnId: TxnId,
        private val events: MutableList<String>,
        private val throwOnPrepare: Boolean = false,
        private val throwOnCommit: Boolean = false
    ) : TransactionResource {
        override fun start() {
            events.add("$name:start")
        }
        override fun end() {
            events.add("$name:end")
        }
        override fun getName() = name
        override fun getXid() = EcosXid.create(txnId, name.toByteArray())
        override fun prepareCommit(): CommitPrepareStatus {
            events.add("$name:prepareCommit")
            if (throwOnPrepare) error("prepare failed for $name")
            return CommitPrepareStatus.PREPARED
        }
        override fun commitPrepared() {
            events.add("$name:commitPrepared")
            if (throwOnCommit) error("commit failed for $name")
        }
        override fun onePhaseCommit() {
            events.add("$name:onePhaseCommit")
        }
        override fun rollback() {
            events.add("$name:rollback")
        }
        override fun dispose() {
            events.add("$name:dispose")
        }
    }

    private class InMemoryTwoPhaseCommitRepo : TwoPhaseCommitRepo {

        val calls = CopyOnWriteArrayList<String>()
        val commitErrors = mutableMapOf<TxnId, Map<String, Throwable>>()
        var ownRecoveryData: RecoveryData? = null
        var globalRecoveryData: RecoveryData? = null
        var preparedData: TxnCommitData? = null
        private var ownRecoveryReturned = false
        private var globalRecoveryReturned = false

        override fun beforePrepare(txnId: TxnId, data: TxnCommitData) {
            calls.add("beforePrepare:$txnId")
            preparedData = TxnCommitData(data.apps.mapValues { it.value.toSet() }, data.actions.toMap())
        }

        override fun beforeCommit(txnId: TxnId, appsToCommit: Set<String>) {
            calls.add("beforeCommit:$txnId")
        }

        override fun afterCommit(txnId: TxnId, committedApps: Set<String>, errors: Map<String, Throwable>) {
            calls.add("afterCommit:$txnId")
            if (errors.isNotEmpty()) {
                commitErrors[txnId] = errors
            }
            // Clear recovery data after successful recovery
            if (errors.isEmpty() && ownRecoveryData?.txnId == txnId) {
                ownRecoveryData = null
            }
        }

        override fun beforeRollback(txnId: TxnId, appsToRollback: Set<String>) {
            calls.add("beforeRollback:$txnId")
        }

        override fun afterRollback(txnId: TxnId, rolledBackApps: Set<String>, errors: Map<String, Throwable>) {
            calls.add("afterRollback:$txnId")
            if (errors.isEmpty() && ownRecoveryData?.txnId == txnId) {
                ownRecoveryData = null
            }
        }

        override fun getRecoveryData(txnId: TxnId): RecoveryData? {
            return if (ownRecoveryData?.txnId == txnId) ownRecoveryData else null
        }

        override fun findDataToRecover(): RecoveryData? {
            if (globalRecoveryReturned) return null
            globalRecoveryReturned = true
            return globalRecoveryData
        }

        override fun findOwnDataToRecover(exclusions: List<TxnId>): RecoveryData? {
            val data = ownRecoveryData ?: return null
            if (exclusions.contains(data.txnId)) return null
            if (ownRecoveryReturned) return null
            ownRecoveryReturned = true
            return data
        }
    }
}
