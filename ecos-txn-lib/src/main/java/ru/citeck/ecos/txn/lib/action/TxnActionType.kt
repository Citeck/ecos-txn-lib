package ru.citeck.ecos.txn.lib.action

enum class TxnActionType(val observationId: String) {
    BEFORE_COMMIT("before-commit"),
    AFTER_COMMIT("after-commit"),
    AFTER_ROLLBACK("after-rollback")
}
