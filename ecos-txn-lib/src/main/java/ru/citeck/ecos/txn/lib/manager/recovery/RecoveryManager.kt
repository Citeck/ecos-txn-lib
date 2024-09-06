package ru.citeck.ecos.txn.lib.manager.recovery

import ru.citeck.ecos.txn.lib.transaction.xid.EcosXid

/**
 * Interface for managing the recovery process of prepared transactions.
 * The `RecoveryManager` is responsible for committing or rolling back prepared transactions (Xid's)
 * and registering recoverable storage systems.
 */
interface RecoveryManager {

    /**
     * Commits the specified collection of prepared Xid's.
     * This method ensures that the resources associated with the given Xid's are committed
     * as part of the recovery process, typically after a system failure or restart.
     *
     * @param xids A collection of `EcosXid` instances representing the prepared resources to be committed.
     */
    fun commitPrepared(xids: Collection<EcosXid>)

    /**
     * Rolls back the specified collection of prepared Xid's.
     * This method undoes the preparation of the resources associated with the given Xid's,
     * rolling them back to maintain consistency during the recovery process.
     *
     * @param xids A collection of `EcosXid` instances representing the prepared resources to be rolled back.
     */
    fun rollbackPrepared(xids: Collection<EcosXid>)

    /**
     * Registers a recoverable storage system that can be used during the recovery process.
     * This method associates the provided storage system with the recovery manager, allowing it
     * to access and manage prepared transactions stored in recoverable storage.
     *
     * @param storage The `RecoverableStorage` instance to be registered for managing prepared Xid's during recovery.
     */
    fun registerStorage(storage: RecoverableStorage)
}
