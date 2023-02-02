package ru.citeck.ecos.txn.lib.transaction

enum class TransactionStatus {
    NEW,
    ACTIVE,
    PREPARING,
    PREPARED,
    COMMITTING,
    COMMITTED,
    ROLLING_BACK,
    ROLLED_BACK,
    DISPOSED,
    NO_TRANSACTION
}
