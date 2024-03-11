package ru.citeck.ecos.txn.lib.transaction.obs

import ru.citeck.ecos.micrometer.obs.EcosObsContext
import ru.citeck.ecos.txn.lib.transaction.Transaction
import ru.citeck.ecos.txn.lib.transaction.TransactionSynchronization

class EcosTxnSyncObsContext {

    class BeforeCompletion(
        val synchronizations: List<TransactionSynchronization>,
        val transaction: Transaction
    ) : EcosObsContext(NAME) {

        companion object {
            const val NAME = "ecos.txn.sync.before-completion"
        }
    }

    class AfterCompletion(
        val synchronizations: List<TransactionSynchronization>,
        val transaction: Transaction
    ) : EcosObsContext(NAME) {

        companion object {
            const val NAME = "ecos.txn.sync.after-completion"
        }
    }
}
