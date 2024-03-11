package ru.citeck.ecos.txn.lib.commit.obs

import ru.citeck.ecos.micrometer.obs.EcosObsContext
import ru.citeck.ecos.txn.lib.commit.TxnCommitData
import ru.citeck.ecos.txn.lib.manager.TransactionManager
import ru.citeck.ecos.txn.lib.transaction.TxnId

class TxnCommitObsContext(
    val txnId: TxnId,
    val data: TxnCommitData,
    val txnLevel: Int,
    val manager: TransactionManager
) : EcosObsContext(NAME) {

    companion object {
        const val NAME = "ecos.txn.commit"
    }
}
