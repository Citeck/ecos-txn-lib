package ru.citeck.ecos.txn.lib.manager.api.server.action

import ru.citeck.ecos.txn.lib.transaction.TxnId

class TmOnePhaseCommitAction : AbstractTmRemoteAction<TmOnePhaseCommitAction.Data>() {

    companion object {
        const val TYPE = "one-phase-commit"
    }

    override fun execute(data: Data, apiVer: Int) {
        manager.getManagedTransaction(data.txnId).onePhaseCommit()
    }

    override fun getType(): String {
        return TYPE
    }

    data class Data(
        val txnId: TxnId
    )
}
