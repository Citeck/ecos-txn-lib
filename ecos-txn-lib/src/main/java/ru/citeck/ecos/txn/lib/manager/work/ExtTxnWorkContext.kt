package ru.citeck.ecos.txn.lib.manager.work

interface ExtTxnWorkContext {

    /**
     * Stop transactional work and do resources cleaning
     *
     * @return true if work has transactional state and false otherwise
     */
    fun stopWork(): Boolean
}
