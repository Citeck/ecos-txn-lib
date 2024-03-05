package ru.citeck.ecos.txn.lib.manager.api.server.action

import ru.citeck.ecos.txn.lib.transaction.TransactionStatus
import ru.citeck.ecos.txn.lib.transaction.TxnId

class TmGetStatusAction : AbstractTmRemoteAction<TmGetStatusAction.Data>() {

    companion object {
        const val TYPE = "get-status"
    }

    override fun execute(data: Data, apiVer: Int): GetStatusResp {
        return GetStatusResp(manager.getStatus(data.txnId))
    }

    override fun getType(): String {
        return TYPE
    }

    data class Data(
        val txnId: TxnId
    )

    data class GetStatusResp(
        val status: TransactionStatus
    )
}
