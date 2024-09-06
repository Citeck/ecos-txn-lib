package ru.citeck.ecos.txn.lib.transaction

import ru.citeck.ecos.txn.lib.action.TxnActionRef
import ru.citeck.ecos.txn.lib.action.TxnActionType
import ru.citeck.ecos.txn.lib.resource.TransactionResource
import ru.citeck.ecos.txn.lib.transaction.xid.EcosXid
import java.time.Instant

/**
 * Interface representing a transactional context, managing resources, actions, and the status of the transaction.
 */
interface Transaction {

    /**
     * Retrieves a resource by its key, or adds a new resource if none exists for the given key.
     *
     * @param key The key associated with the resource.
     * @param resource A function that takes the key and transaction ID,
     *                 and returns the resource to be added if not present.
     * @return The `TransactionResource` instance associated with the key.
     */
    fun <K : Any, T : TransactionResource> getOrAddRes(key: K, resource: (K, TxnId) -> T): T

    /**
     * Registers a collection of Xid's associated with a specific application for the transaction.
     *
     * @param appName The name of the application registering the Xid's.
     * @param xids A collection of `EcosXid` instances to be registered with the transaction.
     */
    fun registerXids(appName: String, xids: Collection<EcosXid>)

    /**
     * Executes a transactional action identified by its action ID.
     *
     * @param actionId The identifier of the action to be executed.
     */
    fun executeAction(actionId: Int)

    /**
     * Adds an action to the transaction for a specific action type.
     *
     * @param type The type of the transactional action (before commit, after commit, or after rollback).
     * @param action A reference to the action to be added.
     */
    fun addAction(type: TxnActionType, action: TxnActionRef)

    /**
     * Adds an action to the transaction for a specific action type, with an execution order.
     *
     * @param type The type of the transactional action (before commit, after commit, or after rollback).
     * @param order The execution order of the action, determining when it should be executed relative to others.
     * @param action The action to be executed.
     */
    fun addAction(type: TxnActionType, order: Float, action: () -> Unit)

    /**
     * @deprecated Use the version of `addAction` without the async flag.
     * Adds an action to the transaction, with an execution order and an async flag.
     *
     * @param type The type of the transactional action (before commit, after commit, or after rollback).
     * @param order The execution order of the action.
     * @param async flag do nothing
     * @param action The action to be executed.
     */
    @Deprecated(
        "use addAction without async flag",
        replaceWith = ReplaceWith("this.addAction(type, order, action)")
    )
    fun addAction(type: TxnActionType, order: Float, async: Boolean, action: () -> Unit)

    /**
     * Registers a synchronization callback to be executed at specific points during the transaction lifecycle.
     *
     * The registered `TransactionSynchronization` will have its `beforeCompletion` method called
     * before the transaction is committed or rolled back, allowing for resource cleanup or other operations
     * before the transaction completes. After the transaction is committed or rolled back, the
     * `afterCompletion` method will be invoked with the final `TransactionStatus`, allowing for additional
     * actions to be performed post-transaction.
     *
     * @param sync A `TransactionSynchronization` instance containing the callbacks for `beforeCompletion`
     *             and `afterCompletion` during commit or rollback events.
     */
    fun registerSync(sync: TransactionSynchronization)

    /**
     * Checks if the transaction is read-only.
     *
     * @return `true` if the transaction is read-only, `false` otherwise.
     */
    fun isReadOnly(): Boolean

    /**
     * Retrieves the current status of the transaction.
     *
     * @return The `TransactionStatus` of the transaction.
     */
    fun getStatus(): TransactionStatus

    /**
     * Retrieves the unique identifier of the transaction.
     *
     * @return The `TxnId` associated with this transaction.
     */
    fun getId(): TxnId

    /**
     * Checks if the transaction has been completed.
     *
     * @return `true` if the transaction is completed, `false` otherwise.
     */
    fun isCompleted(): Boolean

    /**
     * Checks if the transaction contains no resources or transactional actions.
     *
     * @return `true` if the transaction is empty, `false` otherwise.
     */
    fun isEmpty(): Boolean

    /**
     * Checks if the transaction is idle (i.e., not actively processing any operations).
     *
     * @return `true` if the transaction is idle, `false` otherwise.
     */
    fun isIdle(): Boolean

    /**
     * Retrieves the last active time of the transaction.
     *
     * @return The `Instant` representing the last time the transaction was active.
     */
    fun getLastActiveTime(): Instant

    /**
     * Retrieves data associated with the given key from the transaction context.
     *
     * @param key The key associated with the data.
     * @return The data associated with the key, or `null` if not present.
     */
    fun <T : Any> getData(key: Any): T?

    /**
     * Retrieves or computes data associated with the given key from the transaction context.
     * If the data is not present, the provided compute function is used to generate it.
     *
     * @param key The key associated with the data.
     * @param compute A function to compute the data if it's not present.
     * @return The data associated with the key.
     */
    fun <K : Any, T : Any> getData(key: K, compute: (K) -> T): T
}
