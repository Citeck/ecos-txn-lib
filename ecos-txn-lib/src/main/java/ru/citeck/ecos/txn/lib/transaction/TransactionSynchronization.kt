package ru.citeck.ecos.txn.lib.transaction

interface TransactionSynchronization {

    /**
     * Invoked before transaction commit/rollback.
     * Can perform resource cleanup *before* transaction completion.
     */
    fun beforeCompletion() {}

    /**
     * Invoked after transaction commit/rollback.
     * Can perform resource cleanup *after* transaction completion.
     */
    fun afterCompletion(status: TransactionStatus) {}
}
