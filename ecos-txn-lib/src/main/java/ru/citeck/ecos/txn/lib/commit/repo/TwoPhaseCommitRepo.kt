package ru.citeck.ecos.txn.lib.commit.repo

import ru.citeck.ecos.txn.lib.commit.RecoveryData
import ru.citeck.ecos.txn.lib.commit.TxnCommitData
import ru.citeck.ecos.txn.lib.transaction.TxnId

/**
 * Repository interface for managing the state of transactions in a Two-Phase Commit (2PC) protocol.
 * The `TransactionManager` uses this repository to store information during the commit process.
 * In the event of a failure, a new transaction manager instance can recover the transaction
 * from the stored information and continue processing.
 */
interface TwoPhaseCommitRepo {

    /**
     * Stores the transaction data before the prepare phase of the 2PC.
     * This method is called when the transaction manager is ready to prepare the transaction for commit.
     *
     * @param txnId Unique identifier of the transaction.
     * @param data Contains mapping of application names to sets of Xid's (from XA architecture),
     *             representing the resources involved, as well as action descriptors to be executed
     *             at various phases (before commit, after commit, after rollback).
     */
    fun beforePrepare(txnId: TxnId, data: TxnCommitData)

    /**
     * Stores information before the commit phase of the 2PC.
     * This method is called after a successful prepare phase, and the transaction is ready to be committed.
     *
     * @param txnId Unique identifier of the transaction.
     * @param appsToCommit Set of application names that need to be committed as part of the transaction.
     */
    fun beforeCommit(txnId: TxnId, appsToCommit: Set<String>)

    /**
     * Stores the result after the commit phase of the 2PC.
     * This method is called after the transaction has been successfully committed or partially committed with errors.
     *
     * @param txnId Unique identifier of the transaction.
     * @param committedApps Set of application names that were successfully committed.
     * @param errors Map of application names and associated errors that occurred during commit.
     */
    fun afterCommit(txnId: TxnId, committedApps: Set<String>, errors: Map<String, Throwable>)

    /**
     * Stores information before the rollback phase of the 2PC.
     * This method is called when the transaction is being rolled back,
     * typically due to failure in the prepare or commit phase.
     *
     * @param txnId Unique identifier of the transaction.
     * @param appsToRollback Set of application names that need to be rolled back.
     */
    fun beforeRollback(txnId: TxnId, appsToRollback: Set<String>)

    /**
     * Stores the result after the rollback phase of the 2PC.
     * This method is called after the transaction has been successfully
     * rolled back or partially rolled back with errors.
     *
     * @param txnId Unique identifier of the transaction.
     * @param rolledBackApps Set of application identifiers that were successfully rolled back.
     * @param errors Map of application identifiers and associated errors that occurred during rollback.
     */
    fun afterRollback(txnId: TxnId, rolledBackApps: Set<String>, errors: Map<String, Throwable>)

    /**
     * Retrieves the stored data necessary for recovering a transaction.
     * This method is used when a new transaction manager instance is started,
     * and it needs to recover and continue a previously incomplete transactions.
     *
     * @param txnId Unique identifier of the transaction.
     * @return The recovery data for the transaction, or `null` if no data is available for the given transaction ID.
     */
    fun getRecoveryData(txnId: TxnId): RecoveryData?

    /**
     * Finds data for transactions that are eligible for recovery.
     * This method is used to discover transactions that require recovery after a transaction manager failure.
     *
     * @return Recovery data for a transaction that needs recovery, or `null` if no such data is found.
     */
    fun findDataToRecover(): RecoveryData?
}
