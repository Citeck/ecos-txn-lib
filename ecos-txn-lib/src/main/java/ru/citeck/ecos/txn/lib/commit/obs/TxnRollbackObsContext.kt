package ru.citeck.ecos.txn.lib.commit.obs

import ru.citeck.ecos.micrometer.obs.EcosObsContext
import ru.citeck.ecos.txn.lib.action.TxnActionId
import ru.citeck.ecos.txn.lib.manager.TransactionManager
import ru.citeck.ecos.txn.lib.transaction.TxnId

class TxnRollbackObsContext(
    val txnId: TxnId,
    val apps: Collection<String>,
    val actions: List<TxnActionId>,
    val rollbackError: Throwable,
    val txnLevel: Int,
    val manager: TransactionManager
) : EcosObsContext(NAME) {

    companion object {
        const val NAME = "ecos.txn.rollback"
    }
}
