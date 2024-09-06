package ru.citeck.ecos.txn.lib.resource

import ru.citeck.ecos.txn.lib.transaction.xid.EcosXid

/**
 * Interface representing a resource that participates in a transaction.
 * A `TransactionResource` handles the lifecycle of its involvement in the transaction,
 * including preparing, committing, and rolling back operations.
 */
interface TransactionResource {

    /**
     * Starts the resource's involvement in the transaction.
     * This method is called at the beginning of the resource's participation in the transaction.
     */
    fun start()

    /**
     * Ends the resource's involvement in the transaction.
     * This method is called after the resource has completed its participation in the transaction.
     */
    fun end()

    /**
     * Retrieves the name of the resource.
     * The name is typically used to identify the resource during transaction operations.
     *
     * @return The name of the resource as a `String`.
     */
    fun getName(): String

    /**
     * Prepares the resource for commit as part of the two-phase commit process.
     * This method is called during the prepare phase to ensure the resource is ready to commit.
     *
     * @return A `CommitPrepareStatus` indicating the result of the prepare phase.
     */
    fun prepareCommit(): CommitPrepareStatus

    /**
     * Commits the resource that has been prepared during the two-phase commit process.
     * This method finalizes the commit of the resource after it has been prepared successfully.
     */
    fun commitPrepared()

    /**
     * Performs a one-phase commit of the resource.
     * This method is called when the transaction is committed in a single phase.
     */
    fun onePhaseCommit()

    /**
     * Retrieves the Xid (transaction identifier) associated with this resource.
     * The Xid is used to uniquely identify the resource's participation in the transaction.
     *
     * @return The `EcosXid` representing the transaction identifier for this resource.
     */
    fun getXid(): EcosXid

    /**
     * Rolls back the resource, undoing any changes made during the transaction.
     * This method is called if the transaction is aborted or rolled back.
     */
    fun rollback()

    /**
     * Disposes of the resource, releasing any associated resources and cleaning up its state.
     * This method is typically called after the resource is no longer needed in the transaction.
     */
    fun dispose()
}
