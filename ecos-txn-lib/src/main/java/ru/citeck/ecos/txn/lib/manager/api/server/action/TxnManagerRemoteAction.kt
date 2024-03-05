package ru.citeck.ecos.txn.lib.manager.api.server.action

import ru.citeck.ecos.txn.lib.manager.TransactionManagerImpl

interface TxnManagerRemoteAction<T : Any> {

    fun init(manager: TransactionManagerImpl)

    fun execute(data: T, apiVer: Int): Any

    fun getType(): String
}
