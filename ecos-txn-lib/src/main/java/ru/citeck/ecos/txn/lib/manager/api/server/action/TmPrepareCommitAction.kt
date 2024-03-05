package ru.citeck.ecos.txn.lib.manager.api.server.action

import ru.citeck.ecos.txn.lib.resource.CommitPrepareStatus
import ru.citeck.ecos.txn.lib.transaction.TxnId
import ru.citeck.ecos.txn.lib.transaction.xid.EcosXid

class TmPrepareCommitAction : AbstractTmRemoteAction<TmPrepareCommitAction.Data>() {

    companion object {
        const val TYPE = "prepare-commit"
    }

    override fun execute(data: Data, apiVer: Int): Any {
        val result = manager.prepareCommitFromExtManager(
            data.txnId,
            apiVer > 0 // recovering appeared at ver.1
        )
        return if (apiVer <= 1) {
            val status = if (result.isNotEmpty()) {
                CommitPrepareStatus.PREPARED
            } else {
                CommitPrepareStatus.NOTHING_TO_COMMIT
            }
            RespV0(status)
        } else {
            Resp(result)
        }
    }

    override fun getType(): String {
        return TYPE
    }

    data class Data(
        val txnId: TxnId
    )

    data class Resp(
        val preparedXids: List<EcosXid>
    )

    data class RespV0(
        val status: CommitPrepareStatus
    )
}
