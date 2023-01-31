package ru.citeck.ecos.txn.lib.transaction

import ru.citeck.ecos.txn.lib.action.TxnActionRef
import ru.citeck.ecos.txn.lib.action.TxnActionType
import ru.citeck.ecos.txn.lib.resource.TransactionResource

interface Transaction {

    fun <K : Any, T : TransactionResource> getOrAddRes(key: K, resource: (K, TxnId) -> T): T

    fun executeAction(actionId: Int)

    fun addAction(type: TxnActionType, action: TxnActionRef)

    fun addAction(type: TxnActionType, order: Float, async: Boolean, action: () -> Unit)

    fun registerSync(sync: TransactionSynchronization)

    fun isReadOnly(): Boolean

    fun getStatus(): TransactionStatus

    fun getId(): TxnId

    fun isCompleted(): Boolean

    fun isEmpty(): Boolean

    fun <T : Any> getData(key: Any): T?

    fun <K : Any, T : Any> getData(key: K, compute: (K) -> T): T
}
