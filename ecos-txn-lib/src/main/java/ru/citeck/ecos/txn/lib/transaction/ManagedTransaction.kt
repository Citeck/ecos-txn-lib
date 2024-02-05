package ru.citeck.ecos.txn.lib.transaction

import ru.citeck.ecos.txn.lib.resource.CommitPrepareStatus

interface ManagedTransaction : Transaction {

    fun start()

    fun <T> doWithinTxn(managerCtx: TxnManagerContext, readOnly: Boolean, action: () -> T): T

    fun <T> doWithinTxn(readOnly: Boolean, action: () -> T): T

    fun getResourcesNames(): List<String>

    fun prepareCommit(): CommitPrepareStatus

    fun commitPrepared()

    fun onePhaseCommit()

    fun rollback(): List<Throwable>

    fun dispose()
}
