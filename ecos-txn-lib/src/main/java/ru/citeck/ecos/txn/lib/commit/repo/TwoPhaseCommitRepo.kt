package ru.citeck.ecos.txn.lib.commit.repo

import ru.citeck.ecos.txn.lib.commit.RecoveryData
import ru.citeck.ecos.txn.lib.commit.TxnCommitData
import ru.citeck.ecos.txn.lib.transaction.TxnId

interface TwoPhaseCommitRepo {

    fun beforePrepare(txnId: TxnId, data: TxnCommitData)

    fun beforeCommit(txnId: TxnId, appsToCommit: Set<String>)

    fun afterCommit(txnId: TxnId, committedApps: Set<String>, errors: Map<String, Throwable>)

    fun beforeRollback(txnId: TxnId, appsToRollback: Set<String>)

    fun afterRollback(txnId: TxnId, rolledBackApps: Set<String>, errors: Map<String, Throwable>)

    fun getRecoveryData(txnId: TxnId): RecoveryData?

    fun findDataToRecover(): RecoveryData?
}
