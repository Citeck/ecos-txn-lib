package ru.citeck.ecos.txn.lib.manager.api.server.action

import ru.citeck.ecos.txn.lib.transaction.TxnId

class TmExecActionAction : AbstractTmRemoteAction<TmExecActionAction.Data>() {

    companion object {
        const val TYPE = "exec-action"
    }

    override fun execute(data: Data, apiVer: Int) {
        manager.getManagedTransaction(data.txnId).executeAction(data.actionId)
    }

    override fun getType(): String {
        return TYPE
    }

    data class Data(
        val txnId: TxnId,
        val actionId: Int
    )
}
