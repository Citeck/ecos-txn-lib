package ru.citeck.ecos.txn.lib.manager.action.obs

import ru.citeck.ecos.micrometer.obs.EcosObsContext
import ru.citeck.ecos.txn.lib.action.TxnActionType
import ru.citeck.ecos.txn.lib.manager.TransactionManager
import ru.citeck.ecos.txn.lib.transaction.TxnId

class TxnActionsObsContext(
    val txnId: TxnId,
    val type: TxnActionType,
    val manager: TransactionManager
) : EcosObsContext("ecos.txn.actions.${type.observationId}")
