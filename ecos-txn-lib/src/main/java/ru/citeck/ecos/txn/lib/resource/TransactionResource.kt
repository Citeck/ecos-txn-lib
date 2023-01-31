package ru.citeck.ecos.txn.lib.resource

interface TransactionResource {

    fun start()

    fun end()

    fun getName(): String

    fun prepareCommit(): CommitPrepareStatus

    fun commitPrepared()

    fun onePhaseCommit()

    fun rollback()

    fun dispose()
}
