package ru.citeck.ecos.txn.lib.resource

import ru.citeck.ecos.txn.lib.transaction.xid.EcosXid

interface TransactionResource {

    fun start()

    fun end()

    fun getName(): String

    fun prepareCommit(): CommitPrepareStatus

    fun commitPrepared()

    fun onePhaseCommit()

    fun getXid(): EcosXid

    fun rollback()

    fun dispose()
}
