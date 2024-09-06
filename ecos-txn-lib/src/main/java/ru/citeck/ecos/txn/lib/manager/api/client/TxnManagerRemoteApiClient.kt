package ru.citeck.ecos.txn.lib.manager.api.client

import ru.citeck.ecos.txn.lib.commit.TxnCommitData
import ru.citeck.ecos.txn.lib.transaction.TransactionStatus
import ru.citeck.ecos.txn.lib.transaction.TxnId
import ru.citeck.ecos.txn.lib.transaction.xid.EcosXid

/**
 * Remote API client interface for managing distributed transactions in a 2PC protocol.
 * This interface defines the API methods for coordinating commits, rollbacks,
 * and recovery across different applications.
 */
interface TxnManagerRemoteApiClient {

    companion object {
        /**
         * Current API version used by the client.
         */
        const val CURRENT_API_VERSION = 2

        /**
         * Version of the coordinate commit API.
         */
        const val COORDINATE_COMMIT_VER = 2
    }

    /**
     * Coordinates the commit process for the specified application and transaction.
     * This method initiates the commit process and handles transactional actions.
     *
     * @param app The name of application which should coordinate commit.
     * @param txnId Unique identifier of the transaction.
     * @param data Contains mappings of application names to sets of Xid's (from XA architecture)
     *             representing the resources involved, as well as action descriptors.
     * @param txnLevel The current level of the transaction, incremented for new transactions initiated
     *                 by after-commit or after-rollback actions.
     *                 Prevents endless recursive transaction creation.
     */
    fun coordinateCommit(app: String, txnId: TxnId, data: TxnCommitData, txnLevel: Int)

    /**
     * Recovers the commit process for the specified application and transaction.
     * This method is used to re-attempt committing previously prepared Xid's in case of failure or crash.
     *
     * @param app The application name involved in the transaction.
     * @param txnId Unique identifier of the transaction.
     * @param xids Set of Xid's representing resources that need to be committed.
     */
    fun recoveryCommit(app: String, txnId: TxnId, xids: Set<EcosXid>)

    /**
     * Recovers the rollback process for the specified application and transaction.
     * This method rolls back the Xid's that were previously prepared.
     *
     * @param app The application name involved in the transaction.
     * @param txnId Unique identifier of the transaction.
     * @param xids Set of Xid's representing resources that need to be rolled back.
     */
    fun recoveryRollback(app: String, txnId: TxnId, xids: Set<EcosXid>)

    /**
     * Disposes of the transaction, releasing all resources
     * and clearing transactional data for the specified application.
     *
     * @param app The application name involved in the transaction.
     * @param txnId Unique identifier of the transaction.
     */
    fun disposeTxn(app: String, txnId: TxnId)

    /**
     * Executes a one-phase commit for a simple transaction where prepare and commit are combined in one step.
     *
     * @param app The application name involved in the transaction.
     * @param txnId Unique identifier of the transaction.
     */
    fun onePhaseCommit(app: String, txnId: TxnId)

    /**
     * Prepares the transaction for commit and returning a list of prepared Xid's.
     *
     * @param app The application name involved in the transaction.
     * @param txnId Unique identifier of the transaction.
     * @return List of Xid's that are prepared for commit.
     */
    fun prepareCommit(app: String, txnId: TxnId): List<EcosXid>

    /**
     * Commits a transaction that has already been prepared, ensuring all prepared Xid's are committed.
     *
     * @param app The application name involved in the transaction.
     * @param txnId Unique identifier of the transaction.
     */
    fun commitPrepared(app: String, txnId: TxnId)

    /**
     * Executes a specific transaction action by its ID for the given application and transaction.
     *
     * @param app The application name involved in the transaction.
     * @param txnId Unique identifier of the transaction.
     * @param actionId The ID of the action to execute.
     */
    fun executeTxnAction(app: String, txnId: TxnId, actionId: Int)

    /**
     * Rolls back the transaction for the specified application and transaction.
     *
     * @param app The application name involved in the transaction.
     * @param txnId Unique identifier of the transaction.
     * @param cause Optional error or cause that triggered the rollback.
     */
    fun rollback(app: String, txnId: TxnId, cause: Throwable?)

    /**
     * Retrieves the current status of the transaction for the given application.
     *
     * @param app The application name involved in the transaction.
     * @param txnId Unique identifier of the transaction.
     * @return The current status of the transaction.
     */
    fun getTxnStatus(app: String, txnId: TxnId): TransactionStatus

    /**
     * Checks if the specified application is available for API calls.
     *
     * @param app The application name to check.
     * @return `true` if the application is available, otherwise `false`.
     */
    fun isAppAvailable(app: String): Boolean

    /**
     * Verifies if the specified API version is supported by the given application.
     *
     * @param app The application name to check.
     * @param version The API version to verify.
     * @return The result indicating whether the API version is supported.
     */
    fun isApiVersionSupported(app: String, version: Int): ApiVersionRes
}
