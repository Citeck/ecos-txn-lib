package ru.citeck.ecos.txn.lib.manager.api.server.action

import ru.citeck.ecos.txn.lib.transaction.TxnId

class TmCommitPreparedAction : AbstractTmRemoteAction<TmCommitPreparedAction.Data>() {

    companion object {
        const val TYPE = "commit-prepared"
    }

    override fun execute(data: Data, apiVer: Int) {
        manager.getManagedTransaction(data.txnId).commitPrepared()
    }

    override fun getType(): String {
        return TYPE
    }

    data class Data(
        val txnId: TxnId
    )
}
