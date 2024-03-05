package ru.citeck.ecos.txn.lib.manager.api.server.action

import ru.citeck.ecos.txn.lib.transaction.TxnId

class TmRollbackTxnAction : AbstractTmRemoteAction<TmRollbackTxnAction.Data>() {

    companion object {
        const val TYPE = "rollback"
    }

    override fun execute(data: Data, apiVer: Int) {
        manager.getManagedTransaction(data.txnId).rollback(null)
    }

    override fun getType(): String {
        return TYPE
    }

    data class Data(
        val txnId: TxnId
    )
}
