package ru.citeck.ecos.txn.lib

import jakarta.transaction.Status
import jakarta.transaction.Synchronization
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import ru.citeck.ecos.test.commons.EcosWebAppApiMock
import ru.citeck.ecos.txn.lib.action.TxnActionRef
import ru.citeck.ecos.txn.lib.action.TxnActionType
import ru.citeck.ecos.txn.lib.manager.EcosTxnProps
import ru.citeck.ecos.txn.lib.manager.TransactionManagerImpl
import ru.citeck.ecos.txn.lib.manager.TransactionPolicy
import ru.citeck.ecos.txn.lib.manager.api.client.TxnManagerRemoteApiClient
import ru.citeck.ecos.txn.lib.resource.type.xa.JavaXaTxnManagerAdapter
import ru.citeck.ecos.txn.lib.support.RoutingRemoteApiClient
import ru.citeck.ecos.txn.lib.transaction.Transaction
import ru.citeck.ecos.txn.lib.transaction.TransactionSynchronization
import ru.citeck.ecos.txn.lib.transaction.TxnId
import ru.citeck.ecos.txn.lib.transaction.TxnNesting
import ru.citeck.ecos.txn.lib.transaction.ctx.TxnManagerContext
import ru.citeck.ecos.txn.lib.transaction.xid.EcosXid
import javax.transaction.xa.XAResource

/**
 * Interposed synchronizations of a participant, as a managed pool registers one per borrowed connection,
 * share one remote action of the root.
 */
class InterposedSynchronizationTest {

    companion object {
        private const val ROOT = "app-a"
        private const val PARTICIPANT = "app-b"
        private const val COMMITTED = Status.STATUS_COMMITTED
        private const val ROLLED_BACK = Status.STATUS_ROLLEDBACK
    }

    private val managers = mutableListOf<TransactionManagerImpl>()
    private val events = mutableListOf<String>()
    private val apps = TwoApps()

    @AfterEach
    fun tearDown() {
        managers.forEach { it.shutdown() }
    }

    @Test
    fun `interposed synchronizations of a participant cost the root one remote action`() {
        val borrows = 10

        apps.inParticipantTxn { pool ->
            repeat(borrows) { pool.registerInterposedSynchronization(sync("$it")) }
        }

        assertThat(apps.participantExecActions()).isEqualTo(1)
        // the participant is the only app with resources, so the root commits it in one phase
        assertThat(events).containsExactlyElementsOf(
            listOf("xa:start") +
                (0 until borrows).map { "before:$it" } +
                listOf("xa:end", "xa:commit") +
                (0 until borrows).map { "after:$it:$COMMITTED" }
        )
    }

    @Test
    fun `interposed synchronization registered during beforeCompletion joins the running batch`() {
        apps.inParticipantTxn { pool ->
            pool.registerInterposedSynchronization(
                sync("0", before = { pool.registerInterposedSynchronization(sync("2")) })
            )
            pool.registerInterposedSynchronization(sync("1"))
        }

        assertThat(apps.participantExecActions()).isEqualTo(1)
        assertThat(events).containsExactly(
            "xa:start",
            "before:0",
            "before:1",
            "before:2",
            "xa:end",
            "xa:commit",
            "after:0:$COMMITTED",
            "after:1:$COMMITTED",
            "after:2:$COMMITTED"
        )
    }

    @Test
    fun `interposed synchronization registered after its batch ran gets a new batch before the resource ends`() {
        TxnContext.doInTxn {
            val rootTxn = TxnContext.getTxn()
            apps.inParticipant(rootTxn) { pool ->
                pool.transaction!!.enlistResource(recordingResource())
                pool.registerInterposedSynchronization(sync("0"))
            }
            // runs after the batch of the participant: its order is the same and it is registered later
            rootTxn.registerSync(object : TransactionSynchronization {
                override fun beforeCompletion() {
                    apps.inParticipant(rootTxn) { pool ->
                        pool.registerInterposedSynchronization(sync("late"))
                    }
                }
            })
        }

        assertThat(apps.participantExecActions()).isEqualTo(2)
        // the phases of the commit depend on the root's own participation, so only the order is pinned
        assertThat(events).containsSubsequence(
            "xa:start",
            "before:0",
            "before:late",
            "xa:end",
            "xa:commit",
            "after:0:$COMMITTED",
            "after:late:$COMMITTED"
        )
    }

    @Test
    fun `interposed synchronizations of the calls and pools of one transaction share a batch`() {
        val secondPool = JavaXaTxnManagerAdapter(apps.participantAdapter.props)

        TxnContext.doInTxn {
            val rootTxn = TxnContext.getTxn()
            apps.inParticipant(rootTxn) { pool ->
                pool.transaction!!.enlistResource(recordingResource())
                pool.registerInterposedSynchronization(sync("0"))
            }
            apps.inParticipant(rootTxn) {
                secondPool.registerInterposedSynchronization(sync("1"))
            }
        }

        assertThat(apps.participantExecActions()).isEqualTo(1)
        assertThat(events).containsExactly(
            "xa:start",
            "before:0",
            "before:1",
            "xa:end",
            "xa:commit",
            "after:0:$COMMITTED",
            "after:1:$COMMITTED"
        )
    }

    @Test
    fun `synchronizations registered with the transaction keep a remote action each`() {
        apps.inParticipantTxn { pool ->
            val jta = pool.transaction!!
            jta.registerSynchronization(sync("direct-0"))
            pool.registerInterposedSynchronization(sync("interposed-0"))
            pool.registerInterposedSynchronization(sync("interposed-1"))
            jta.registerSynchronization(sync("direct-1"))
        }

        // two direct synchronizations and one batch
        assertThat(apps.participantExecActions()).isEqualTo(3)
        assertThat(events).containsExactly(
            "xa:start",
            "before:direct-0",
            "before:interposed-0",
            "before:interposed-1",
            "before:direct-1",
            "xa:end",
            "xa:commit",
            "after:direct-0:$COMMITTED",
            "after:interposed-0:$COMMITTED",
            "after:interposed-1:$COMMITTED",
            "after:direct-1:$COMMITTED"
        )
    }

    @Test
    fun `failed interposed beforeCompletion rolls back and reports rollback to every member`() {
        val failure = IllegalStateException("flush failed")

        assertThatThrownBy {
            apps.inParticipantTxn { pool ->
                pool.registerInterposedSynchronization(sync("0", before = { throw failure }))
                pool.registerInterposedSynchronization(sync("1"))
            }
        }.isSameAs(failure)

        assertThat(events).containsExactly(
            "xa:start",
            "before:0",
            "xa:rollback",
            "after:0:$ROLLED_BACK",
            "after:1:$ROLLED_BACK"
        )
    }

    @Test
    fun `failed afterCompletion of one interposed synchronization does not stop the others`() {
        apps.inParticipantTxn { pool ->
            pool.registerInterposedSynchronization(
                sync("0", after = { throw IllegalStateException("after failed") })
            )
            pool.registerInterposedSynchronization(sync("1"))
        }

        assertThat(events).containsExactly(
            "xa:start",
            "before:0",
            "before:1",
            "xa:end",
            "xa:commit",
            "after:0:$COMMITTED",
            "after:1:$COMMITTED"
        )
    }

    // ==================== Helpers ====================

    /**
     * Root and participant in one JVM. Like the web API, a call of the participant runs in its part
     * of the root transaction and hands the actions and Xids it registers to the root.
     */
    private inner class TwoApps {

        private val managerByApp = mutableMapOf<String, TransactionManagerImpl>()
        private var participantActions = 0
        private val participantApi = EcosWebAppApiMock(PARTICIPANT)

        /** The pool's view of the participant's transactions, valid inside [inParticipant] */
        val participantAdapter = JavaXaTxnManagerAdapter(participantApi.getProperties())

        /** Hands to the root what the participant registers, in its calls and in the actions the root executes */
        private lateinit var rootContext: TxnManagerContext

        private val delegate = RoutingRemoteApiClient(managerByApp)
        private val client = object : TxnManagerRemoteApiClient by delegate {
            override fun executeTxnAction(app: String, txnId: TxnId, actionId: Int) {
                if (app == PARTICIPANT) {
                    participantActions++
                }
                inApp(app, txnId) { delegate.executeTxnAction(app, txnId, actionId) }
            }
        }

        init {
            managerByApp[ROOT] = createManager(EcosWebAppApiMock(ROOT), client)
            managerByApp[PARTICIPANT] = createManager(participantApi, client)
            TxnContext.setManager(managerByApp.getValue(ROOT))
        }

        /** Remote actions the root executed on the participant */
        fun participantExecActions(): Int {
            return participantActions
        }

        /** A root transaction in which the participant enlists [resource], as a pool does on the first borrow */
        fun inParticipantTxn(
            resource: XAResource = recordingResource(),
            action: (JavaXaTxnManagerAdapter) -> Unit
        ) {
            TxnContext.doInTxn {
                inParticipant(TxnContext.getTxn()) { pool ->
                    pool.transaction!!.enlistResource(resource)
                    action(pool)
                }
            }
        }

        fun inParticipant(rootTxn: Transaction, action: (JavaXaTxnManagerAdapter) -> Unit) {
            rootContext = object : TxnManagerContext {
                override fun registerAction(type: TxnActionType, actionRef: TxnActionRef) {
                    rootTxn.addAction(type, actionRef)
                }
                override fun registerXids(appName: String, xids: Collection<EcosXid>) {
                    rootTxn.registerXids(appName, xids)
                }
            }
            inApp(PARTICIPANT, rootTxn.getId()) { action(participantAdapter) }
        }

        private fun inApp(app: String, txnId: TxnId, action: () -> Unit) {
            val manager = managerByApp.getValue(app)
            val managerBefore = TxnContext.getManager()
            TxnContext.setManager(manager)
            try {
                manager.doInExtTxn(txnId, rootContext, TransactionPolicy.REQUIRED, false, TxnNesting.ROOT) { action() }
            } finally {
                TxnContext.setManager(managerBefore)
            }
        }
    }

    private fun createManager(appApi: EcosWebAppApiMock, client: TxnManagerRemoteApiClient): TransactionManagerImpl {
        val manager = TransactionManagerImpl()
        manager.init(appApi, EcosTxnProps(), remoteClient = client)
        managers.add(manager)
        return manager
    }

    /** Records the resource's completion among the events of the synchronizations, to observe their order */
    private fun recordingResource(): XAResource {
        return XaResourceAdapterTest.RecordingXAResource(onCall = { events.add("xa:${it.method}") })
    }

    private fun sync(name: String, before: () -> Unit = {}, after: () -> Unit = {}): Synchronization {
        return object : Synchronization {
            override fun beforeCompletion() {
                events.add("before:$name")
                before()
            }
            override fun afterCompletion(status: Int) {
                events.add("after:$name:$status")
                after()
            }
        }
    }
}
