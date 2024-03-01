package ru.citeck.ecos.txn.lib.resource.type.xa

import mu.KotlinLogging
import ru.citeck.ecos.txn.lib.resource.CommitPrepareStatus
import ru.citeck.ecos.txn.lib.resource.TransactionResource
import ru.citeck.ecos.txn.lib.transaction.TxnId
import ru.citeck.ecos.txn.lib.transaction.xid.EcosXid
import javax.transaction.xa.XAResource

class JavaXaTxnResourceAdapter(
    private val resource: XAResource,
    private val name: String,
    private val txnId: TxnId,
    currentAppName: String,
    currentAppInstanceId: String
) : TransactionResource {

    companion object {
        private val log = KotlinLogging.logger {}
    }

    private val xid = EcosXid.create(txnId, currentAppName, currentAppInstanceId)

    override fun getXid(): EcosXid {
        return xid
    }

    override fun start() {
        resource.start(xid, XAResource.TMNOFLAGS)
    }

    override fun end() {
        resource.end(xid, XAResource.TMSUCCESS)
    }

    override fun getName(): String {
        return name
    }

    override fun prepareCommit(): CommitPrepareStatus {
        val result = resource.prepare(xid)
        return if (result and XAResource.XA_RDONLY > 0) {
            log.info { "[$txnId][$name] Prepare with READ ONLY result" }
            CommitPrepareStatus.NOTHING_TO_COMMIT
        } else {
            log.info { "[$txnId][$name] Prepare with PREPARED result" }
            CommitPrepareStatus.PREPARED
        }
    }

    override fun commitPrepared() {
        resource.commit(xid, false)
    }

    override fun onePhaseCommit() {
        resource.commit(xid, true)
    }

    override fun rollback() {
        resource.rollback(xid)
    }

    override fun dispose() {
    }
}
