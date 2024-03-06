package ru.citeck.ecos.txn.lib.commit

import ru.citeck.ecos.txn.lib.action.TxnActionId
import ru.citeck.ecos.txn.lib.transaction.TxnId

interface CommitCoordinator {

    fun commitRoot(txnId: TxnId, data: TxnCommitData, txnLevel: Int)

    fun rollbackRoot(
        txnId: TxnId,
        apps: Collection<String>,
        actions: List<TxnActionId>?,
        error: Throwable,
        txnLevel: Int
    )

    fun disposeRoot(txnId: TxnId, apps: Collection<String>, mainError: Throwable?, disposeCurrentApp: Boolean = true)

    fun runTxnRecovering()
}
