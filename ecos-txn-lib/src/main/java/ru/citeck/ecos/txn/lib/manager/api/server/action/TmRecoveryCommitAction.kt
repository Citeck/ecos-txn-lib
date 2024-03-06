package ru.citeck.ecos.txn.lib.manager.api.server.action

import ru.citeck.ecos.txn.lib.transaction.TxnId
import ru.citeck.ecos.txn.lib.transaction.xid.EcosXid

class TmRecoveryCommitAction : AbstractTmRemoteAction<TmRecoveryCommitAction.Data>() {

    companion object {
        const val TYPE = "recovery-commit"
    }

    override fun execute(data: Data, apiVer: Int) {
        manager.recoveryCommit(data.txnId, data.xids)
    }

    override fun getType(): String {
        return TYPE
    }

    data class Data(
        val txnId: TxnId,
        val xids: List<EcosXid>
    )
}
