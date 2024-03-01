package ru.citeck.ecos.txn.lib.commit

import ru.citeck.ecos.txn.lib.commit.repo.TwoPhaseCommitStatus
import ru.citeck.ecos.txn.lib.transaction.TxnId

data class RecoveryData(
    val txnId: TxnId,
    val data: TxnCommitData,
    val status: TwoPhaseCommitStatus,
    val appsToProcess: Set<String>,
    val ownerApp: String,
    val appRoutes: Map<String, String>
)
