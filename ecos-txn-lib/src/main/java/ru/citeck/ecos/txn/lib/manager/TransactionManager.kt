package ru.citeck.ecos.txn.lib.manager

import ru.citeck.ecos.txn.lib.resource.CommitPrepareStatus
import ru.citeck.ecos.txn.lib.transaction.Transaction
import ru.citeck.ecos.txn.lib.transaction.TransactionStatus
import ru.citeck.ecos.txn.lib.transaction.TxnId
import ru.citeck.ecos.txn.lib.transaction.TxnManagerContext

interface TransactionManager {

    fun <T> doInTxn(policy: TransactionPolicy, readOnly: Boolean?, action: () -> T): T

    fun <T> doInExtTxn(
        extTxnId: TxnId,
        extCtx: TxnManagerContext,
        policy: TransactionPolicy,
        readOnly: Boolean,
        action: () -> T
    ): T

    fun getTransaction(txnId: TxnId): Transaction?

    fun executeAction(txnId: TxnId, actionId: Int)

    fun prepareCommit(txnId: TxnId): CommitPrepareStatus

    fun commitPrepared(txnId: TxnId)

    fun onePhaseCommit(txnId: TxnId)

    fun rollback(txnId: TxnId)

    fun getCurrentTransaction(): Transaction?

    fun dispose(txnId: TxnId)

    fun getStatus(txnId: TxnId): TransactionStatus
}
