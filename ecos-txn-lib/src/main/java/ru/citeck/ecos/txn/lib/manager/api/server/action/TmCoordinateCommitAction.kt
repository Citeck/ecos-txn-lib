package ru.citeck.ecos.txn.lib.manager.api.server.action

import ru.citeck.ecos.txn.lib.commit.TxnCommitData
import ru.citeck.ecos.txn.lib.transaction.TxnId

class TmCoordinateCommitAction : AbstractTmRemoteAction<TmCoordinateCommitAction.Data>() {

    companion object {
        const val TYPE = "coordinate-commit"
    }

    override fun execute(data: Data, apiVer: Int) {
        manager.coordinateCommit(data.txnId, data.data, data.txnLevel)
    }

    override fun getType(): String {
        return TYPE
    }

    data class Data(
        val txnId: TxnId,
        val data: TxnCommitData,
        val txnLevel: Int
    )
}
