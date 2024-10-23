package ru.citeck.ecos.txn.lib.commit.repo

import ru.citeck.ecos.txn.lib.commit.RecoveryData
import ru.citeck.ecos.txn.lib.commit.TxnCommitData
import ru.citeck.ecos.txn.lib.transaction.TxnId

object NoopTwoPhaseCommitRepo : TwoPhaseCommitRepo {

    override fun beforePrepare(txnId: TxnId, data: TxnCommitData) {}

    override fun beforeCommit(txnId: TxnId, appsToCommit: Set<String>) {}

    override fun afterCommit(txnId: TxnId, committedApps: Set<String>, errors: Map<String, Throwable>) {}

    override fun beforeRollback(txnId: TxnId, appsToRollback: Set<String>) {}

    override fun afterRollback(txnId: TxnId, rolledBackApps: Set<String>, errors: Map<String, Throwable>) {}

    override fun findDataToRecover(exclusions: List<TxnId>): RecoveryData? = null

    override fun getRecoveryData(txnId: TxnId): RecoveryData? = null
}
