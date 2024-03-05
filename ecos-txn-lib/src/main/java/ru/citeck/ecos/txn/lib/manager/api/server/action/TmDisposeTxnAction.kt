package ru.citeck.ecos.txn.lib.manager.api.server.action

import ru.citeck.ecos.txn.lib.transaction.TxnId

class TmDisposeTxnAction : AbstractTmRemoteAction<TmDisposeTxnAction.Data>() {

    companion object {
        const val TYPE = "dispose"
    }

    override fun execute(data: Data, apiVer: Int) {
        manager.dispose(data.txnId)
    }

    override fun getType(): String {
        return TYPE
    }

    data class Data(
        val txnId: TxnId
    )
}
