package ru.citeck.ecos.txn.lib

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import ru.citeck.ecos.micrometer.EcosMicrometerContext
import ru.citeck.ecos.test.commons.EcosWebAppApiMock
import ru.citeck.ecos.txn.lib.manager.EcosTxnProps
import ru.citeck.ecos.txn.lib.manager.TransactionManagerImpl
import ru.citeck.ecos.txn.lib.manager.TxnManagerJob
import ru.citeck.ecos.txn.lib.manager.api.client.TxnManagerRemoteApiClient
import ru.citeck.ecos.txn.lib.transaction.ManagedTransaction
import ru.citeck.ecos.txn.lib.transaction.TransactionImpl
import ru.citeck.ecos.txn.lib.transaction.TransactionStatus
import ru.citeck.ecos.txn.lib.transaction.TxnId
import java.lang.reflect.Proxy
import java.time.Instant

class TxnHealthJobTest {

    private val managers = mutableListOf<TransactionManagerImpl>()

    @AfterEach
    fun tearDown() {
        managers.forEach { it.shutdown() }
    }

    @Test
    fun `busy and recently active transactions refresh liveness without remote requests`() {
        val fixture = Fixture()
        val busy = fixture.add("remote", idle = false)
        val recent = fixture.add("remote", activeTime = Instant.now())
        fixture.run()
        assertThat(busy.info.lastAliveTime).isGreaterThan(System.currentTimeMillis() - 5_000)
        assertThat(recent.info.lastAliveTime).isEqualTo(recent.activeTime.toEpochMilli())
        assertThat(fixture.calls).isEmpty()
        assertThat(busy.disposals + recent.disposals).isZero()
    }

    @Test
    fun `expired local transaction disposes while recent local context survives without discovery`() {
        val fixture = Fixture()
        val expired = fixture.add("health")
        val recent = fixture.add("health", lastAliveTime = System.currentTimeMillis() - 10_000)
        fixture.run()
        assertThat(expired.disposals).isEqualTo(1)
        assertThat(fixture.manager.transactionsById).doesNotContainKey(expired.id).containsKey(recent.id)
        assertThat(recent.disposals).isZero()
        assertThat(fixture.calls).isEmpty()
    }

    @Test
    fun `remote owner status refreshes liveness and targets exact origin instance`() {
        val fixture = Fixture()
        val txn = fixture.add("origin")
        fixture.ownerStatus = TransactionStatus.ACTIVE
        fixture.run()
        assertThat(fixture.calls).containsExactly("available:origin:old", "status:origin:old")
        assertThat(txn.info.lastAliveTime).isGreaterThan(System.currentTimeMillis() - 5_000)
        assertThat(txn.disposals).isZero()
    }

    @Test
    fun `delegated local context checks configured coordinator instead of origin`() {
        val fixture = Fixture()
        val txn = fixture.add("health", delegatedTo = "coordinator:owner")
        fixture.run()
        assertThat(fixture.calls).containsExactly("available:coordinator:owner", "status:coordinator:owner")
        assertThat(txn.disposals).isZero()
    }

    @Test
    fun `definitive no-transaction expires an otherwise not-yet-expired remote context`() {
        val fixture = Fixture()
        val txn = fixture.add("origin", lastAliveTime = System.currentTimeMillis() - 30_000)
        fixture.ownerStatus = TransactionStatus.NO_TRANSACTION
        fixture.run()
        assertThat(txn.info.lastAliveTime).isZero()
        assertThat(txn.disposals).isEqualTo(1)
        assertThat(fixture.manager.transactionsById).doesNotContainKey(txn.id)
    }

    @Test
    fun `unavailable owner and status errors do not refresh liveness or prematurely dispose`() {
        for (unavailable in listOf(true, false)) {
            val fixture = Fixture()
            val lastAlive = System.currentTimeMillis() - 30_000
            val txn = fixture.add("origin", lastAliveTime = lastAlive)
            fixture.available = !unavailable
            fixture.statusError = IllegalStateException("status unavailable")
            fixture.run()
            assertThat(txn.info.lastAliveTime).isEqualTo(lastAlive)
            assertThat(txn.disposals).isZero()
            assertThat(fixture.calls).hasSize(if (unavailable) 1 else 2)
        }
    }

    @Test
    fun `expired unreachable context disposes and failed disposal remains retryable without blocking peers`() {
        val fixture = Fixture()
        fixture.available = false
        val failing = fixture.add("origin", disposalFailure = true)
        val other = fixture.add("origin")
        fixture.run()
        assertThat(failing.disposals).isEqualTo(1)
        assertThat(other.disposals).isEqualTo(1)
        assertThat(fixture.manager.transactionsById).containsKey(failing.id).doesNotContainKey(other.id)
        fixture.run()
        assertThat(failing.disposals).isEqualTo(2)
    }

    private inner class Fixture {
        val calls = mutableListOf<String>()
        var available = true
        var ownerStatus = TransactionStatus.ACTIVE
        var statusError: Throwable? = null
        val manager = TransactionManagerImpl()

        init {
            val remote = Proxy.newProxyInstance(
                javaClass.classLoader,
                arrayOf(TxnManagerRemoteApiClient::class.java)
            ) { _, method, args ->
                when (method.name) {
                    "isAppAvailable" -> {
                        calls.add("available:${args!![0]}")
                        available
                    }
                    "getTxnStatus" -> {
                        calls.add("status:${args!![0]}")
                        statusError?.let { throw it }
                        ownerStatus
                    }
                    else -> error("Unexpected remote operation ${method.name}")
                }
            } as TxnManagerRemoteApiClient
            manager.init(EcosWebAppApiMock("health"), EcosTxnProps(), remoteClient = remote)
            managers.add(manager)
        }

        fun add(
            app: String,
            idle: Boolean = true,
            activeTime: Instant = Instant.EPOCH,
            lastAliveTime: Long = 0L,
            delegatedTo: String = "",
            disposalFailure: Boolean = false
        ): Context {
            val id = TxnId.create(app, "old")
            val context = Context(id, activeTime)
            val delegate = TransactionImpl(id, "health", false, EcosMicrometerContext.NOOP)
            delegate.start()
            val transaction = object : ManagedTransaction by delegate {
                override fun isIdle(): Boolean {
                    return idle
                }
                override fun getLastActiveTime(): Instant {
                    return activeTime
                }
                override fun dispose() {
                    context.disposals++
                    if (disposalFailure) {
                        error("dispose failed")
                    }
                    delegate.dispose()
                }
            }
            context.info = TransactionManagerImpl.TransactionInfo(
                transaction,
                commitDelegatedToApp = delegatedTo,
                lastAliveTime = lastAliveTime
            )
            manager.transactionsById[id] = context.info
            return context
        }

        fun run() {
            val method = TxnManagerJob::class.java.getDeclaredMethod("disposeInactiveTransactions")
            method.isAccessible = true
            method.invoke(TxnManagerJob(manager))
        }
    }

    private class Context(val id: TxnId, val activeTime: Instant) {
        lateinit var info: TransactionManagerImpl.TransactionInfo
        var disposals = 0
    }
}
