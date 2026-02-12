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
import ru.citeck.ecos.txn.lib.manager.api.client.ApiVersionRes
import ru.citeck.ecos.txn.lib.manager.api.client.TxnManagerRemoteApiClient
import ru.citeck.ecos.txn.lib.resource.CommitPrepareStatus
import ru.citeck.ecos.txn.lib.resource.TransactionResource
import ru.citeck.ecos.txn.lib.transaction.TransactionImpl
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
        private var ownRecoveryReturned = false
        private var globalRecoveryReturned = false

        override fun beforePrepare(txnId: TxnId, data: TxnCommitData) {
            calls.add("beforePrepare:$txnId")
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

    private class RoutingRemoteApiClient(
        private val registry: Map<String, TransactionManagerImpl>
    ) : TxnManagerRemoteApiClient {
        override fun prepareCommit(app: String, txnId: TxnId) =
            getManager(app).prepareCommitFromExtManager(txnId, true)
        override fun commitPrepared(app: String, txnId: TxnId) =
            getManager(app).getManagedTransaction(txnId).commitPrepared()
        override fun onePhaseCommit(app: String, txnId: TxnId) =
            getManager(app).getManagedTransaction(txnId).onePhaseCommit()
        override fun rollback(app: String, txnId: TxnId, cause: Throwable?) =
            getManager(app).getManagedTransaction(txnId).rollback(cause)
        override fun disposeTxn(app: String, txnId: TxnId) =
            getManager(app).dispose(txnId)
        override fun coordinateCommit(app: String, txnId: TxnId, data: TxnCommitData, txnLevel: Int) =
            getManager(app).coordinateCommit(txnId, data, txnLevel)
        override fun recoveryCommit(app: String, txnId: TxnId, xids: Set<EcosXid>) =
            getManager(app).recoveryCommit(txnId, xids)
        override fun recoveryRollback(app: String, txnId: TxnId, xids: Set<EcosXid>) =
            getManager(app).recoveryRollback(txnId, xids)
        override fun getTxnStatus(app: String, txnId: TxnId) =
            getManager(app).getStatus(txnId)
        override fun executeTxnAction(app: String, txnId: TxnId, actionId: Int) =
            getManager(app).getManagedTransaction(txnId).executeAction(actionId)
        override fun isApiVersionSupported(app: String, version: Int) =
            if (resolveManager(app) != null) ApiVersionRes.SUPPORTED else ApiVersionRes.APP_NOT_AVAILABLE
        override fun isAppAvailable(app: String) = resolveManager(app) != null
        private fun getManager(app: String) =
            resolveManager(app) ?: error("No manager for '$app'")
        private fun resolveManager(app: String): TransactionManagerImpl? =
            registry[app] ?: registry.entries.firstOrNull { app.startsWith(it.key) }?.value
    }
}
