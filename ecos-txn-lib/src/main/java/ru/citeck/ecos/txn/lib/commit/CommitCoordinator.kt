package ru.citeck.ecos.txn.lib.commit

import ru.citeck.ecos.txn.lib.action.TxnActionId
import ru.citeck.ecos.txn.lib.transaction.TxnId

/**
 * Interface for coordinating the commit, rollback, and recovery of transactions.
 * It manages the root transaction commit process, including handling after-commit and after-rollback actions.
 */
interface CommitCoordinator {

    /**
     * Initiates the commit process for the transaction.
     * This method coordinates the final commit for all resources involved in the transaction.
     * The `txnLevel` is incremented for each new transaction initiated by after-commit actions,
     * and if the level reaches limit, an error is thrown to prevent endless recursive transaction creation.
     *
     * @param txnId Unique identifier of the transaction.
     * @param data Contains mappings of application names to sets of Xid's (from XA architecture)
     *             representing the resources involved, as well as action descriptors to be executed
     *             at various phases (before commit, after commit, after rollback).
     * @param txnLevel The current level of the transaction, used to track iterations of transaction actions.
     *                 Transactions initiated by after-commit actions increment this level.
     *                 If `txnLevel` reaches limit, an error is thrown.
     */
    fun commitRoot(txnId: TxnId, data: TxnCommitData, txnLevel: Int)

    /**
     * Initiates the rollback process for the transaction.
     * This method rolls back the transaction, releasing resources and executing the necessary rollback actions.
     * Similar to the commit process, the `txnLevel` tracks transactions initiated by after-rollback actions.
     *
     * @param txnId Unique identifier of the transaction.
     * @param apps Collection of application names that need to be rolled back.
     * @param actions List of action identifiers (`TxnActionId`) to execute after rollback,
     *                or `null` if none are specified.
     * @param error The error that caused the rollback, providing context for failure.
     * @param txnLevel The current level of the transaction, used to track iterations
     *                 of transaction actions during rollback.
     *                 If `txnLevel` reaches limit, an error is thrown.
     */
    fun rollbackRoot(
        txnId: TxnId,
        apps: Collection<String>,
        actions: List<TxnActionId>?,
        error: Throwable,
        txnLevel: Int
    )

    /**
     * Cleans up after the transaction has completed or failed.
     * This method disposes of the transaction, either by releasing resources or executing custom disposal logic.
     *
     * @param txnId Unique identifier of the transaction.
     * @param apps Collection of application names involved in the transaction.
     * @param mainError Optional error that may have occurred during transaction processing.
     * @param disposeCurrentApp Whether to dispose of the current application as part of the cleanup. Defaults to `true`.
     */
    fun disposeRoot(txnId: TxnId, apps: Collection<String>, mainError: Throwable?, disposeCurrentApp: Boolean = true)

    /**
     * Triggers the recovery process for transactions that require recovery.
     * This method runs the recovery logic for incomplete transactions that need to be restored after failure.
     */
    fun runTxnRecovering()
}
