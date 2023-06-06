package ru.citeck.ecos.txn.lib

import mu.KotlinLogging
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.test.commons.EcosWebAppApiMock
import ru.citeck.ecos.txn.lib.manager.TransactionManagerImpl
import ru.citeck.ecos.txn.lib.resource.CommitPrepareStatus
import ru.citeck.ecos.txn.lib.resource.TransactionResource

class TransactionTest {

    companion object {
        private val log = KotlinLogging.logger {}
    }

    @Test
    fun test() {

        val appApiMock = EcosWebAppApiMock()

        val txnManager = TransactionManagerImpl()
        txnManager.init(appApiMock)
        TxnContext.setManager(txnManager)

        val res0 = CustomRes("res-0")
        val res1 = CustomRes("res-1")

        TxnContext.doInTxn {
            TxnContext.getTxn().getOrAddRes(res0.getName()) { _, _ -> res0 }
            TxnContext.getTxn().getOrAddRes(res1.getName()) { _, _ -> res1 }
        }

        assertThat(res0.status).isEqualTo(ResStatus.COMMITTED)
        assertThat(res1.status).isEqualTo(ResStatus.COMMITTED)
    }

    class CustomRes(private val name: String) : TransactionResource {

        var status: ResStatus = ResStatus.IDLE

        override fun start() {
            changeStatus(setOf(ResStatus.IDLE), ResStatus.ACTIVE)
        }

        override fun end() {
            changeStatus(setOf(ResStatus.ACTIVE), ResStatus.ENDED)
        }

        override fun getName(): String {
            return name
        }

        override fun prepareCommit(): CommitPrepareStatus {
            changeStatus(setOf(ResStatus.ENDED), ResStatus.PREPARED)
            return CommitPrepareStatus.PREPARED
        }

        override fun commitPrepared() {
            changeStatus(setOf(ResStatus.PREPARED), ResStatus.COMMITTED)
        }

        override fun onePhaseCommit() {
            changeStatus(setOf(ResStatus.ENDED), ResStatus.COMMITTED)
        }

        override fun rollback() {
            changeStatus(setOf(ResStatus.ACTIVE, ResStatus.ENDED, ResStatus.PREPARED), ResStatus.ROLLED_BACK)
        }

        override fun dispose() {
            log.info { "[${getName()}] Dispose" }
        }

        fun changeStatus(from: Set<ResStatus>, to: ResStatus) {
            if (!from.contains(status)) {
                error("[${getName()}] Invalid current status: $status. Expected one of: $from")
            }
            log.info { "[${getName()}] Change status from ${this.status} to $to" }
            this.status = to
        }
    }

    enum class ResStatus {
        IDLE,
        ACTIVE,
        ENDED,
        PREPARED,
        COMMITTED,
        ROLLED_BACK
    }
}
