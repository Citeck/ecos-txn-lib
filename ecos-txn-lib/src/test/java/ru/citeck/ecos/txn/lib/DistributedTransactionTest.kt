package ru.citeck.ecos.txn.lib

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import ru.citeck.ecos.micrometer.EcosMicrometerContext
import ru.citeck.ecos.test.commons.EcosWebAppApiMock
import ru.citeck.ecos.txn.lib.action.TxnActionRef
import ru.citeck.ecos.txn.lib.action.TxnActionType
import ru.citeck.ecos.txn.lib.commit.TxnCommitData
import ru.citeck.ecos.txn.lib.manager.EcosTxnProps
import ru.citeck.ecos.txn.lib.manager.TransactionManagerImpl
import ru.citeck.ecos.txn.lib.manager.TransactionPolicy
import ru.citeck.ecos.txn.lib.manager.api.client.ApiVersionRes
import ru.citeck.ecos.txn.lib.manager.api.client.TxnManagerRemoteApiClient
import ru.citeck.ecos.txn.lib.resource.CommitPrepareStatus
import ru.citeck.ecos.txn.lib.resource.TransactionResource
import ru.citeck.ecos.txn.lib.transaction.TransactionImpl
import ru.citeck.ecos.txn.lib.transaction.TransactionStatus
import ru.citeck.ecos.txn.lib.transaction.TxnId
import ru.citeck.ecos.txn.lib.transaction.ctx.TxnManagerContext
import ru.citeck.ecos.txn.lib.transaction.xid.EcosXid
import java.util.concurrent.CopyOnWriteArrayList

class DistributedTransactionTest {

    private val managers = mutableListOf<TransactionManagerImpl>()

    @AfterEach
    fun tearDown() {
        managers.forEach { it.shutdown() }
    }

    // ==================== Test infrastructure ====================

    private class RoutingRemoteApiClient(
        private val registry: Map<String, TransactionManagerImpl>
    ) : TxnManagerRemoteApiClient {

        override fun prepareCommit(app: String, txnId: TxnId): List<EcosXid> {
            return getManager(app).prepareCommitFromExtManager(txnId, true)
        }

        override fun commitPrepared(app: String, txnId: TxnId) {
            getManager(app).getManagedTransaction(txnId).commitPrepared()
        }

        override fun onePhaseCommit(app: String, txnId: TxnId) {
            getManager(app).getManagedTransaction(txnId).onePhaseCommit()
        }

        override fun rollback(app: String, txnId: TxnId, cause: Throwable?) {
            getManager(app).getManagedTransaction(txnId).rollback(cause)
        }

        override fun disposeTxn(app: String, txnId: TxnId) {
            getManager(app).dispose(txnId)
        }

        override fun coordinateCommit(app: String, txnId: TxnId, data: TxnCommitData, txnLevel: Int) {
            getManager(app).coordinateCommit(txnId, data, txnLevel)
        }

        override fun recoveryCommit(app: String, txnId: TxnId, xids: Set<EcosXid>) {
            getManager(app).recoveryCommit(txnId, xids)
        }

        override fun recoveryRollback(app: String, txnId: TxnId, xids: Set<EcosXid>) {
            getManager(app).recoveryRollback(txnId, xids)
        }

        override fun getTxnStatus(app: String, txnId: TxnId): TransactionStatus {
            return getManager(app).getStatus(txnId)
        }

        override fun executeTxnAction(app: String, txnId: TxnId, actionId: Int) {
            getManager(app).getManagedTransaction(txnId).executeAction(actionId)
        }

        override fun isApiVersionSupported(app: String, version: Int): ApiVersionRes {
            return if (resolveManager(app) != null) {
                ApiVersionRes.SUPPORTED
            } else {
                ApiVersionRes.APP_NOT_AVAILABLE
            }
        }

        override fun isAppAvailable(app: String): Boolean {
            return resolveManager(app) != null
        }

        private fun getManager(app: String): TransactionManagerImpl {
            return resolveManager(app)
                ?: error("No manager registered for app '$app'")
        }

        private fun resolveManager(app: String): TransactionManagerImpl? {
            // Try exact match first, then match by app name prefix (ignoring instance id)
            return registry[app] ?: registry.entries.firstOrNull { app.startsWith(it.key) }?.value
        }
    }

    private class TestResource(
        private val name: String,
        private val txnId: TxnId,
        private val events: MutableList<String>,
        private val prepareResult: CommitPrepareStatus = CommitPrepareStatus.PREPARED,
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
            return prepareResult
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

    private data class AppSetup(
        val name: String,
        val manager: TransactionManagerImpl,
        val appMock: EcosWebAppApiMock
    )

    private fun createApp(
        appName: String,
        routingClient: RoutingRemoteApiClient,
        props: EcosTxnProps = EcosTxnProps()
    ): AppSetup {
        val appMock = EcosWebAppApiMock(appName)
        val manager = TransactionManagerImpl()
        manager.init(appMock, props, remoteClient = routingClient)
        managers.add(manager)
        return AppSetup(appName, manager, appMock)
    }

    /**
     * Creates a transaction on the given app and enlists a resource.
     * Returns the collected XIDs for that app.
     */
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
                TestResource(
                    resourceName,
                    id,
                    events,
                    throwOnPrepare = throwOnPrepare,
                    throwOnCommit = throwOnCommit
                )
            }
        }
        return collectedXids
    }

    // ==================== Test scenarios ====================

    @Test
    fun `2PC with coordinator delegation - coordinator app orchestrates commit`() {
        val events = CopyOnWriteArrayList<String>()

        // Build routing client first, then create apps referencing it
        val registryMap = mutableMapOf<String, TransactionManagerImpl>()
        val routingClient = RoutingRemoteApiClient(registryMap)

        val appA = createApp("app-a", routingClient, EcosTxnProps(commitCoordinatorApp = "app-b"))
        val appB = createApp("app-b", routingClient)
        registryMap["app-a"] = appA.manager
        registryMap["app-b"] = appB.manager

        TxnContext.setManager(appA.manager)

        val txnId = TxnId.create("app-a", appA.appMock.appInstanceId)

        val xidsA = setupAppTransaction(appA, txnId, events, "resA")
        val xidsB = setupAppTransaction(appB, txnId, events, "resB")

        val commitData = TxnCommitData(
            mapOf("app-a" to xidsA, "app-b" to xidsB),
            emptyMap()
        )

        // App A delegates to App B as coordinator
        appA.manager.coordinateCommit(txnId, commitData, 0)

        // Both resources went through full 2PC lifecycle
        assertThat(events).contains(
            "resA:prepareCommit",
            "resB:prepareCommit",
            "resA:commitPrepared",
            "resB:commitPrepared",
            "resA:dispose",
            "resB:dispose"
        )
        // Prepare happens before commit
        assertThat(events.indexOf("resA:commitPrepared"))
            .isGreaterThan(events.indexOf("resB:prepareCommit"))
        assertThat(events.indexOf("resB:commitPrepared"))
            .isGreaterThan(events.indexOf("resA:prepareCommit"))
        // No one-phase commit used
        assertThat(events).doesNotContain("resA:onePhaseCommit", "resB:onePhaseCommit")
        // Transactions disposed on both apps
        assertThat(appA.manager.getStatus(txnId)).isEqualTo(TransactionStatus.NO_TRANSACTION)
        assertThat(appB.manager.getStatus(txnId)).isEqualTo(TransactionStatus.NO_TRANSACTION)
    }

    @Test
    fun `2PC without coordinator delegation - initiator coordinates directly`() {
        val events = CopyOnWriteArrayList<String>()

        val registryMap = mutableMapOf<String, TransactionManagerImpl>()
        val routingClient = RoutingRemoteApiClient(registryMap)

        val appA = createApp("app-a", routingClient)
        val appB = createApp("app-b", routingClient)
        registryMap["app-a"] = appA.manager
        registryMap["app-b"] = appB.manager

        TxnContext.setManager(appA.manager)

        val txnId = TxnId.create("app-a", appA.appMock.appInstanceId)

        val xidsA = setupAppTransaction(appA, txnId, events, "resA")
        val xidsB = setupAppTransaction(appB, txnId, events, "resB")

        val commitData = TxnCommitData(
            mapOf("app-a" to xidsA, "app-b" to xidsB),
            emptyMap()
        )

        appA.manager.coordinateCommit(txnId, commitData, 0)

        // Both resources went through 2PC
        assertThat(events).contains(
            "resA:prepareCommit",
            "resB:prepareCommit",
            "resA:commitPrepared",
            "resB:commitPrepared",
            "resA:dispose",
            "resB:dispose"
        )
        assertThat(events).doesNotContain("resA:onePhaseCommit", "resB:onePhaseCommit")

        // All prepares before all commits
        val lastPrepareIdx = maxOf(
            events.indexOf("resA:prepareCommit"),
            events.indexOf("resB:prepareCommit")
        )
        val firstCommitIdx = minOf(
            events.indexOf("resA:commitPrepared"),
            events.indexOf("resB:commitPrepared")
        )
        assertThat(firstCommitIdx).isGreaterThan(lastPrepareIdx)

        // Transactions disposed on both apps
        assertThat(appA.manager.getStatus(txnId)).isEqualTo(TransactionStatus.NO_TRANSACTION)
        assertThat(appB.manager.getStatus(txnId)).isEqualTo(TransactionStatus.NO_TRANSACTION)
    }

    @Test
    fun `distributed rollback on prepare failure - both resources rolled back`() {
        val events = CopyOnWriteArrayList<String>()

        val registryMap = mutableMapOf<String, TransactionManagerImpl>()
        val routingClient = RoutingRemoteApiClient(registryMap)

        val appA = createApp("app-a", routingClient)
        val appB = createApp("app-b", routingClient)
        registryMap["app-a"] = appA.manager
        registryMap["app-b"] = appB.manager

        TxnContext.setManager(appA.manager)

        val txnId = TxnId.create("app-a", appA.appMock.appInstanceId)

        val xidsA = setupAppTransaction(appA, txnId, events, "resA")
        val xidsB = setupAppTransaction(appB, txnId, events, "resB", throwOnPrepare = true)

        val commitData = TxnCommitData(
            mapOf("app-a" to xidsA, "app-b" to xidsB),
            emptyMap()
        )

        assertThatThrownBy {
            appA.manager.coordinateCommit(txnId, commitData, 0)
        }.hasMessageContaining("prepare failed")

        // Both resources should be rolled back
        assertThat(events).contains("resA:rollback", "resB:rollback")
        // No commits happened
        assertThat(events).doesNotContain("resA:commitPrepared", "resB:commitPrepared")
        // Both disposed
        assertThat(events).contains("resA:dispose", "resB:dispose")
    }

    @Test
    fun `distributed rollback on commit failure - error propagated`() {
        val events = CopyOnWriteArrayList<String>()

        val registryMap = mutableMapOf<String, TransactionManagerImpl>()
        val routingClient = RoutingRemoteApiClient(registryMap)

        val appA = createApp("app-a", routingClient)
        val appB = createApp("app-b", routingClient)
        registryMap["app-a"] = appA.manager
        registryMap["app-b"] = appB.manager

        TxnContext.setManager(appA.manager)

        val txnId = TxnId.create("app-a", appA.appMock.appInstanceId)

        val xidsA = setupAppTransaction(appA, txnId, events, "resA")
        val xidsB = setupAppTransaction(appB, txnId, events, "resB", throwOnCommit = true)

        val commitData = TxnCommitData(
            mapOf("app-a" to xidsA, "app-b" to xidsB),
            emptyMap()
        )

        assertThatThrownBy {
            appA.manager.coordinateCommit(txnId, commitData, 0)
        }.hasMessageContaining("commit failed")

        // Both resources were prepared successfully
        assertThat(events).contains("resA:prepareCommit", "resB:prepareCommit")
        // App A committed successfully (order depends on iteration, but at least one committed)
        assertThat(events).contains("resA:commitPrepared")
        // resB's commitPrepared was called but threw
        assertThat(events).contains("resB:commitPrepared")
    }

    @Test
    fun `inner transaction rollback does not affect outer distributed transaction`() {
        val events = CopyOnWriteArrayList<String>()

        val registryMap = mutableMapOf<String, TransactionManagerImpl>()
        val routingClient = RoutingRemoteApiClient(registryMap)

        val appA = createApp("app-a", routingClient)
        val appB = createApp("app-b", routingClient)
        registryMap["app-a"] = appA.manager
        registryMap["app-b"] = appB.manager

        TxnContext.setManager(appA.manager)

        // Use doInNewTxn on app-a for the outer transaction
        appA.manager.doInTxn(
            TransactionPolicy.REQUIRES_NEW,
            false
        ) {
            val outerTxnId = TxnContext.getTxn().getId()
            TxnContext.getTxn().getOrAddRes("outerRes") { _, id ->
                TestResource("outerRes", id, events)
            }

            // Inner REQUIRES_NEW fails
            try {
                appA.manager.doInTxn(
                    TransactionPolicy.REQUIRES_NEW,
                    false
                ) {
                    TxnContext.getTxn().getOrAddRes("innerRes") { _, id ->
                        TestResource("innerRes", id, events)
                    }
                    error("inner fails")
                }
            } catch (_: Exception) {
                // swallow
            }

            // Outer transaction still active after inner rollback
            assertThat(TxnContext.getTxn().getId()).isEqualTo(outerTxnId)
        }

        // Inner was rolled back, outer committed
        assertThat(events).contains("innerRes:rollback")
        assertThat(events).contains("outerRes:onePhaseCommit")
        assertThat(events).doesNotContain("outerRes:rollback")
    }
}
