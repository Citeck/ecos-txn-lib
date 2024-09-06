package ru.citeck.ecos.txn.lib.transaction

import ru.citeck.ecos.txn.lib.resource.TransactionResource
import ru.citeck.ecos.txn.lib.transaction.ctx.TxnManagerContext
import ru.citeck.ecos.txn.lib.transaction.xid.EcosXid

/**
 * Interface representing a managed transaction, extending the base `Transaction` interface.
 * A `ManagedTransaction` includes additional functionality for managing resources, handling
 * transaction lifecycle operations, and executing actions within the transaction context.
 */
interface ManagedTransaction : Transaction {

    /**
     * Retrieves the list of resources participating in this transaction.
     *
     * @return A list of `TransactionResource` instances associated with this transaction.
     */
    fun getResources(): List<TransactionResource>

    /**
     * Starts the managed transaction. This method initiates the transaction lifecycle.
     */
    fun start()

    /**
     * Sets whether the transaction is read-only.
     *
     * @param readOnly A flag indicating whether the transaction should be read-only.
     */
    fun setReadOnly(readOnly: Boolean)

    /**
     * Executes the provided action within the transaction context, using the provided transaction
     * manager context and read-only flag.
     *
     * @param managerCtx The `TxnManagerContext` for the transaction.
     * @param readOnly Specifies whether the transaction should be read-only.
     * @param action The action to be executed within the transaction context.
     * @return The result of the action.
     */
    fun <T> doWithinTxn(managerCtx: TxnManagerContext, readOnly: Boolean, action: () -> T): T

    /**
     * Executes the provided action within the transaction context, using the specified read-only flag.
     *
     * @param readOnly Specifies whether the transaction should be read-only.
     * @param action The action to be executed within the transaction context.
     * @return The result of the action.
     */
    fun <T> doWithinTxn(readOnly: Boolean, action: () -> T): T

    /**
     * Retrieves the names of the resources participating in this transaction.
     *
     * @return A list of resource names as strings.
     */
    fun getResourcesNames(): List<String>

    /**
     * Prepares the transaction for commit as part of the two-phase commit process.
     *
     * @return A list of `EcosXid` representing the prepared resources. If the list is empty,
     *         the transaction can be disposed without further actions.
     */
    fun prepareCommit(): List<EcosXid>

    /**
     * Commits the resources that have been prepared during the two-phase commit process.
     */
    fun commitPrepared()

    /**
     * Commits the transaction in a single phase.
     */
    fun onePhaseCommit()

    /**
     * Rolls back the transaction, undoing any changes made. An optional cause can be provided.
     *
     * @param cause An optional `Throwable` that triggered the rollback, or `null` if none.
     */
    fun rollback(cause: Throwable?)

    /**
     * Disposes of the transaction, releasing resources and cleaning up after the transaction has completed.
     */
    fun dispose()
}
