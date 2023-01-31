package ru.citeck.ecos.txn.lib.manager

import ru.citeck.ecos.txn.lib.transaction.Transaction

interface ActionExecContext {

    fun isReadOnly(): Boolean

    fun getTransaction(): Transaction?

    fun isNewTransaction(): Boolean
}
