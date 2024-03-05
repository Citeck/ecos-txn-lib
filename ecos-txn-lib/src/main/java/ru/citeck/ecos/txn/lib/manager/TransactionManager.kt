package ru.citeck.ecos.txn.lib.manager

import ru.citeck.ecos.txn.lib.commit.TxnCommitData
import ru.citeck.ecos.txn.lib.manager.recovery.RecoveryManager
import ru.citeck.ecos.txn.lib.manager.work.ExtTxnWorkContext
import ru.citeck.ecos.txn.lib.transaction.ManagedTransaction
import ru.citeck.ecos.txn.lib.transaction.Transaction
import ru.citeck.ecos.txn.lib.transaction.TransactionStatus
import ru.citeck.ecos.txn.lib.transaction.TxnId
import ru.citeck.ecos.txn.lib.transaction.ctx.TxnManagerContext
import ru.citeck.ecos.txn.lib.transaction.xid.EcosXid

interface TransactionManager {

    fun coordinateCommit(txnId: TxnId, data: TxnCommitData, txnLevel: Int)

    fun <T> doInTxn(policy: TransactionPolicy, readOnly: Boolean?, action: () -> T): T

    fun <T> doInExtTxn(
        extTxnId: TxnId,
        extCtx: TxnManagerContext,
        policy: TransactionPolicy,
        readOnly: Boolean,
        action: (ExtTxnWorkContext) -> T
    ): T

    fun prepareCommitFromExtManager(txnId: TxnId, managerCanRecoverPreparedTxn: Boolean): List<EcosXid>

    fun getManagedTransactionOrNull(txnId: TxnId): ManagedTransaction?

    fun getManagedTransaction(txnId: TxnId): ManagedTransaction

    fun getTransactionOrNull(txnId: TxnId): Transaction?

    fun dispose(txnId: TxnId)

    fun getCurrentTransaction(): Transaction?

    fun getStatus(txnId: TxnId): TransactionStatus

    fun getRecoveryManager(): RecoveryManager

    fun shutdown()
}
