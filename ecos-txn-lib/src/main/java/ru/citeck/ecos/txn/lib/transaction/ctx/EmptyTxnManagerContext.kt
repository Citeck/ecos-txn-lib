package ru.citeck.ecos.txn.lib.transaction.ctx

import ru.citeck.ecos.txn.lib.action.TxnActionRef
import ru.citeck.ecos.txn.lib.action.TxnActionType
import ru.citeck.ecos.txn.lib.resource.TransactionResource
import ru.citeck.ecos.txn.lib.transaction.xid.EcosXid

object EmptyTxnManagerContext : TxnManagerContext {

    override fun registerAction(type: TxnActionType, actionRef: TxnActionRef) {
        error("TxnManagerContext is empty. Type: $type ActionRef: $actionRef")
    }

    override fun addRemoteXids(appName: String, xids: Set<EcosXid>) {
        error("TxnManagerContext is empty. AppName: $appName ActionRef: $xids")
    }

    override fun onResourceAdded(resource: TransactionResource) {
        error("TxnManagerContext is empty. Resource: ${resource.getName()}")
    }
}
