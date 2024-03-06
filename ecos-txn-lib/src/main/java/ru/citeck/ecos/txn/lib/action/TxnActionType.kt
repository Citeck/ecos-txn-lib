package ru.citeck.ecos.txn.lib.action

enum class TxnActionType(val observationId: String) {
    /**
     * Action before commit. Synchronous
     */
    BEFORE_COMMIT("before-commit"),

    /**
     * Action after commit. Asynchronous
     */
    AFTER_COMMIT("after-commit"),

    /**
     * Action after rollback. Synchronous
     */
    AFTER_ROLLBACK("after-rollback")
}
