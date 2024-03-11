package ru.citeck.ecos.txn.lib.manager.obs

import ru.citeck.ecos.micrometer.obs.EcosObsContext
import ru.citeck.ecos.txn.lib.manager.TransactionManager
import ru.citeck.ecos.txn.lib.transaction.TxnId

class TxnActionObsContext(
    val txnId: TxnId,
    val manager: TransactionManager
) : EcosObsContext(NAME) {

    companion object {
        const val NAME = "ecos.txn.action"
    }
}
