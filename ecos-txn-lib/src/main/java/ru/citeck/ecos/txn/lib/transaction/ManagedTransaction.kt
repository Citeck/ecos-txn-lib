package ru.citeck.ecos.txn.lib.transaction

import ru.citeck.ecos.txn.lib.resource.TransactionResource
import ru.citeck.ecos.txn.lib.transaction.ctx.TxnManagerContext
import ru.citeck.ecos.txn.lib.transaction.xid.EcosXid

interface ManagedTransaction : Transaction {

    fun getResources(): List<TransactionResource>

    fun start()

    fun setReadOnly(readOnly: Boolean)

    fun <T> doWithinTxn(managerCtx: TxnManagerContext, readOnly: Boolean, action: () -> T): T

    fun <T> doWithinTxn(readOnly: Boolean, action: () -> T): T

    fun getResourcesNames(): List<String>

    /**
     * @return prepared resources xids. If this list is empty,
     * then transaction can be disposed without any additional actions
     */
    fun prepareCommit(): List<EcosXid>

    fun commitPrepared()

    fun onePhaseCommit()

    fun rollback(cause: Throwable?)

    fun dispose()
}
