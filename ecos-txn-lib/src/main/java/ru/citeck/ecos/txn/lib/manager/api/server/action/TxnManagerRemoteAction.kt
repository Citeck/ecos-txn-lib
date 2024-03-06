package ru.citeck.ecos.txn.lib.manager.api.server.action

import ru.citeck.ecos.txn.lib.manager.TransactionManager

interface TxnManagerRemoteAction<T : Any> {

    fun init(manager: TransactionManager)

    fun execute(data: T, apiVer: Int): Any

    fun getType(): String
}
