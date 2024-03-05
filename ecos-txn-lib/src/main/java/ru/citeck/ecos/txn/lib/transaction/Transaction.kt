package ru.citeck.ecos.txn.lib.transaction

import ru.citeck.ecos.txn.lib.action.TxnActionRef
import ru.citeck.ecos.txn.lib.action.TxnActionType
import ru.citeck.ecos.txn.lib.resource.TransactionResource
import ru.citeck.ecos.txn.lib.transaction.xid.EcosXid
import java.time.Instant

interface Transaction {

    fun <K : Any, T : TransactionResource> getOrAddRes(key: K, resource: (K, TxnId) -> T): T

    fun registerXids(appName: String, xids: Collection<EcosXid>)

    fun executeAction(actionId: Int)

    fun addAction(type: TxnActionType, action: TxnActionRef)

    fun addAction(type: TxnActionType, order: Float, action: () -> Unit)

    @Deprecated(
        "use addAction without async flag",
        replaceWith = ReplaceWith("this.addAction(type, order, action)")
    )
    fun addAction(type: TxnActionType, order: Float, async: Boolean, action: () -> Unit)

    fun registerSync(sync: TransactionSynchronization)

    fun isReadOnly(): Boolean

    fun getStatus(): TransactionStatus

    fun getId(): TxnId

    fun isCompleted(): Boolean

    /**
     * Return true when transaction doesn't contains
     * resources and transactional actions
     */
    fun isEmpty(): Boolean

    fun isIdle(): Boolean

    fun getLastActiveTime(): Instant

    fun <T : Any> getData(key: Any): T?

    fun <K : Any, T : Any> getData(key: K, compute: (K) -> T): T
}
