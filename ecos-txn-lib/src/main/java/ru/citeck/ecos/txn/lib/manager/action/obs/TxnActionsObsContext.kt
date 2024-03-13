package ru.citeck.ecos.txn.lib.manager.action.obs

import ru.citeck.ecos.micrometer.obs.EcosObsContext
import ru.citeck.ecos.txn.lib.action.TxnActionId
import ru.citeck.ecos.txn.lib.action.TxnActionType
import ru.citeck.ecos.txn.lib.manager.TransactionManager
import ru.citeck.ecos.txn.lib.transaction.TxnId

class TxnActionsObsContext(
    val txnId: TxnId,
    val type: TxnActionType,
    val manager: TransactionManager,
    val actions: List<TxnActionId>
) : EcosObsContext("ecos.txn.actions.${type.observationId}") {

    var actionsTime = emptyMap<TxnActionId, Long>()
}
