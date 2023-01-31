package ru.citeck.ecos.txn.lib.transaction

import ru.citeck.ecos.txn.lib.action.TxnActionRef
import ru.citeck.ecos.txn.lib.action.TxnActionType
import ru.citeck.ecos.txn.lib.resource.TransactionResource

interface TxnManagerContext {

    fun registerAction(type: TxnActionType, actionRef: TxnActionRef)

    fun addResource(resource: TransactionResource): Boolean
}
