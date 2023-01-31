package ru.citeck.ecos.txn.lib.action

enum class TxnActionType {
    BEFORE_COMMIT,
    AFTER_COMMIT,
    AFTER_ROLLBACK
}
