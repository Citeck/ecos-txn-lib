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

/**
 * Interface for managing transactions, including coordinating commits, executing transactional actions,
 * handling external transactions, and performing recovery operations.
 * The `TransactionManager` handles both local and distributed transactions.
 */
interface TransactionManager {

    /**
     * Coordinates the commit process for the specified transaction.
     *
     * @param txnId Unique identifier of the transaction.
     * @param data Contains mappings of application names to sets of Xid's (from XA architecture)
     *             representing the resources involved, as well as action descriptors.
     * @param txnLevel The current level of the transaction, incremented for new transactions initiated
     *                 by after-commit actions. Prevents endless recursive transaction creation.
     */
    fun coordinateCommit(txnId: TxnId, data: TxnCommitData, txnLevel: Int)

    /**
     * Executes the provided action within the context of a transaction.
     * The transaction is created based on the given `policy` and can be read-only if specified.
     *
     * @param policy The policy determining the transaction boundaries and behavior.
     * @param readOnly Specifies whether the transaction is read-only. `null` indicates the default value,
     *                 which depends on the transaction policy and whether a current transaction exists.
     *                 If a current transaction exists and the policy supports it, the read-only flag
     *                 will be inherited from that transaction. Otherwise, `false` will be used as the default value.
     * @param action The action to execute within the transaction.
     * @return The result of the action executed within the transaction.
     */
    fun <T> doInTxn(policy: TransactionPolicy, readOnly: Boolean?, action: () -> T): T

    /**
     * Executes the provided action within the context of a transaction.
     * This method allows integrating with an externally managed transaction context.
     *
     * @param extTxnId Unique identifier of the external transaction.
     * @param extCtx The context of the external transaction manager.
     * @param policy The policy determining the transaction boundaries and behavior.
     * @param readOnly Whether the transaction is read-only.
     * @param action The action to execute within the external transaction, using an `ExtTxnWorkContext`.
     * @return The result of the action executed within the external transaction.
     */
    fun <T> doInExtTxn(
        extTxnId: TxnId,
        extCtx: TxnManagerContext,
        policy: TransactionPolicy,
        readOnly: Boolean,
        action: (ExtTxnWorkContext) -> T
    ): T

    /**
     * Prepares the commit process for an external transaction manager.
     * This method enlists resources for the commit and returns the list of Xid's that are prepared for commit.
     *
     * @param txnId Unique identifier of the transaction.
     * @param managerCanRecoverPreparedTxn Indicates if the external manager can recover prepared transactions.
     * @return A list of prepared `EcosXid` instances.
     */
    fun prepareCommitFromExtManager(txnId: TxnId, managerCanRecoverPreparedTxn: Boolean): List<EcosXid>

    /**
     * Retrieves the managed transaction for the specified transaction ID, or `null` if it doesn't exist.
     *
     * @param txnId Unique identifier of the transaction.
     * @return The `ManagedTransaction` instance, or `null` if not found.
     */
    fun getManagedTransactionOrNull(txnId: TxnId): ManagedTransaction?

    /**
     * Retrieves the managed transaction for the specified transaction ID.
     *
     * @param txnId Unique identifier of the transaction.
     * @return The `ManagedTransaction` instance.
     * @throws IllegalArgumentException if the transaction is not found.
     */
    fun getManagedTransaction(txnId: TxnId): ManagedTransaction

    /**
     * Retrieves the transaction for the specified transaction ID, or `null` if it doesn't exist.
     *
     * @param txnId Unique identifier of the transaction.
     * @return The `Transaction` instance, or `null` if not found.
     */
    fun getTransactionOrNull(txnId: TxnId): Transaction?

    /**
     * Disposes of the specified transaction, releasing any associated resources and clearing the transaction context.
     *
     * @param txnId Unique identifier of the transaction.
     */
    fun dispose(txnId: TxnId)

    /**
     * Retrieves the current active transaction in the current execution context, if any.
     *
     * @return The current `Transaction` instance, or `null` if none is active.
     */
    fun getCurrentTransaction(): Transaction?

    /**
     * Retrieves the current status of the specified transaction.
     *
     * @param txnId Unique identifier of the transaction.
     * @return The current `TransactionStatus` of the transaction.
     */
    fun getStatus(txnId: TxnId): TransactionStatus

    /**
     * Retrieves the recovery manager associated with this transaction manager.
     * The recovery manager is responsible for handling the recovery of prepared transactions.
     *
     * @return The `RecoveryManager` instance.
     */
    fun getRecoveryManager(): RecoveryManager

    /**
     * Recovers and commits the specified collection of prepared Xid's.
     *
     * @param txnId Unique identifier of the transaction.
     * @param xids A collection of `EcosXid` instances representing the prepared resources to be committed.
     */
    fun recoveryCommit(txnId: TxnId, xids: Collection<EcosXid>)

    /**
     * Recovers and rolls back the specified collection of prepared Xid's.
     *
     * @param txnId Unique identifier of the transaction.
     * @param xids A collection of `EcosXid` instances representing the prepared resources to be rolled back.
     */
    fun recoveryRollback(txnId: TxnId, xids: Collection<EcosXid>)

    /**
     * Shuts down the transaction manager.
     */
    fun shutdown()
}
