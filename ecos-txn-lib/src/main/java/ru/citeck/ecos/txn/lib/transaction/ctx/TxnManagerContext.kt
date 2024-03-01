package ru.citeck.ecos.txn.lib.transaction.ctx

import ru.citeck.ecos.txn.lib.action.TxnActionRef
import ru.citeck.ecos.txn.lib.action.TxnActionType
import ru.citeck.ecos.txn.lib.resource.TransactionResource
import ru.citeck.ecos.txn.lib.transaction.xid.EcosXid

interface TxnManagerContext {

    fun registerAction(type: TxnActionType, actionRef: TxnActionRef)

    fun addRemoteXids(appName: String, xids: Set<EcosXid>)

    /**
     * Called when new transaction resource was added
     */
    fun onResourceAdded(resource: TransactionResource)
}
