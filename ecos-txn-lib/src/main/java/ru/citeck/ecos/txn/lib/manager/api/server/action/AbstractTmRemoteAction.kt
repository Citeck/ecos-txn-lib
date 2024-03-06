package ru.citeck.ecos.txn.lib.manager.api.server.action

import ru.citeck.ecos.txn.lib.manager.TransactionManager

abstract class AbstractTmRemoteAction<T : Any> : TxnManagerRemoteAction<T> {

    protected lateinit var manager: TransactionManager

    override fun init(manager: TransactionManager) {
        this.manager = manager
    }
}
