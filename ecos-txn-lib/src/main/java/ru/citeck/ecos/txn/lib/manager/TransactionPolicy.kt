package ru.citeck.ecos.txn.lib.manager

enum class TransactionPolicy {

    /**
     * Support a current transaction, create a new one if none exists.
     */
    REQUIRED,

    /**
     * Create a new transaction, and suspend the current transaction if one exists.
     */
    REQUIRES_NEW,

    /**
     * Support a current transaction, execute non-transactionally if none exists.
     */
    SUPPORTS,

    /**
     * Execute non-transactionally, suspend the current transaction if one exists.
     */
    NOT_SUPPORTED
}
