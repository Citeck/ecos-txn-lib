package ru.citeck.ecos.txn.lib.transaction

import ru.citeck.ecos.txn.lib.resource.CommitPrepareStatus
import ru.citeck.ecos.txn.lib.resource.TransactionResource
import ru.citeck.ecos.txn.lib.transaction.ctx.TxnManagerContext

interface ManagedTransaction : Transaction {

    fun getResources(): List<TransactionResource>

    fun start()

    fun <T> doWithinTxn(managerCtx: TxnManagerContext, readOnly: Boolean, action: () -> T): T

    fun <T> doWithinTxn(readOnly: Boolean, action: () -> T): T

    fun getResourcesNames(): List<String>

    fun prepareCommit(): CommitPrepareStatus

    fun commitPrepared()

    fun onePhaseCommit()

    fun rollback(cause: Throwable?)

    fun dispose()
}
