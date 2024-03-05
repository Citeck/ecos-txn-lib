package ru.citeck.ecos.txn.lib.manager.api.server.action

import ru.citeck.ecos.txn.lib.manager.TransactionManagerImpl

abstract class AbstractTmRemoteAction<T : Any> : TxnManagerRemoteAction<T> {

    protected lateinit var manager: TransactionManagerImpl

    override fun init(manager: TransactionManagerImpl) {
        this.manager = manager
    }
}
