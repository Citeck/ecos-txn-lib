package ru.citeck.ecos.txn.lib.transaction

interface TransactionSynchronization {

    /**
     * Invoked once before commit. Writable managed transactions run this callback
     * as a before-commit action, while resources may still be enlisted and before
     * the coordinator persists its participant snapshot. Not invoked on rollback.
     */
    fun beforeCompletion() {}

    /**
     * Invoked after transaction commit/rollback.
     * Can perform resource cleanup *after* transaction completion.
     */
    fun afterCompletion(status: TransactionStatus) {}
}
