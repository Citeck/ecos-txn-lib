package ru.citeck.ecos.txn.lib.manager.recovery

import ru.citeck.ecos.txn.lib.transaction.xid.EcosXid

/**
 * Interface for managing the storage of prepared transactions in a recoverable storage system.
 * This interface allows for querying, committing, and rolling back prepared transactions (Xid's)
 * to ensure transactional consistency across system restarts or failures.
 */
interface RecoverableStorage {

    /**
     * Retrieves a list of all prepared Xid's that are stored in the system.
     * These Xid's represent resources that have been prepared for commit
     * but have not yet been committed or rolled back.
     *
     * @return A list of prepared `EcosXid` instances.
     */
    fun getPreparedXids(): List<EcosXid>

    /**
     * Commits the specified prepared Xid.
     * This method finalizes the commit of the resource associated with the given Xid,
     * ensuring transactional consistency.
     *
     * @param xid The `EcosXid` representing the prepared resource to be committed.
     */
    fun commitPrepared(xid: EcosXid)

    /**
     * Rolls back the specified prepared Xid.
     * This method undoes the preparation of the resource associated with the given Xid,
     * rolling back its state.
     *
     * @param xid The `EcosXid` representing the prepared resource to be rolled back.
     */
    fun rollbackPrepared(xid: EcosXid)
}
