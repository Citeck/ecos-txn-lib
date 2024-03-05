package ru.citeck.ecos.txn.lib.transaction.ctx

import ru.citeck.ecos.txn.lib.action.TxnActionRef
import ru.citeck.ecos.txn.lib.action.TxnActionType
import ru.citeck.ecos.txn.lib.transaction.xid.EcosXid

object NoopTxnManagerContext : TxnManagerContext {

    override fun registerAction(type: TxnActionType, actionRef: TxnActionRef) {}

    override fun registerXids(appName: String, xids: Collection<EcosXid>) {}
}
