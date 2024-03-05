package ru.citeck.ecos.txn.lib.transaction.ctx

import ru.citeck.ecos.txn.lib.action.TxnActionRef
import ru.citeck.ecos.txn.lib.action.TxnActionType
import ru.citeck.ecos.txn.lib.transaction.xid.EcosXid

interface TxnManagerContext {

    fun registerAction(type: TxnActionType, actionRef: TxnActionRef)

    fun registerXids(appName: String, xids: Collection<EcosXid>)
}
