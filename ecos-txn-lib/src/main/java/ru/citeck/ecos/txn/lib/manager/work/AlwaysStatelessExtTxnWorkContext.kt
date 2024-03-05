package ru.citeck.ecos.txn.lib.manager.work

object AlwaysStatelessExtTxnWorkContext : ExtTxnWorkContext {
    override fun stopWork(): Boolean {
        return false
    }
}
