package ru.citeck.ecos.txn.lib.manager.api.client

import ru.citeck.ecos.txn.lib.commit.TxnCommitData
import ru.citeck.ecos.txn.lib.transaction.TransactionStatus
import ru.citeck.ecos.txn.lib.transaction.TxnId
import ru.citeck.ecos.txn.lib.transaction.xid.EcosXid

interface TxnManagerRemoteApiClient {

    companion object {
        const val CURRENT_API_VERSION = 2
        const val COORDINATE_COMMIT_VER = 2
    }

    fun coordinateCommit(app: String, txnId: TxnId, data: TxnCommitData, txnLevel: Int)

    fun recoveryCommit(app: String, txnId: TxnId, xids: Set<EcosXid>)

    fun recoveryRollback(app: String, txnId: TxnId, xids: Set<EcosXid>)

    fun disposeTxn(app: String, txnId: TxnId)

    // commit

    fun onePhaseCommit(app: String, txnId: TxnId)

    fun prepareCommit(app: String, txnId: TxnId): List<EcosXid>

    fun commitPrepared(app: String, txnId: TxnId)

    // ///

    fun executeTxnAction(app: String, txnId: TxnId, actionId: Int)

    fun rollback(app: String, txnId: TxnId, cause: Throwable?)

    fun getTxnStatus(app: String, txnId: TxnId): TransactionStatus

    fun isAppAvailable(app: String): Boolean

    fun isApiVersionSupported(app: String, version: Int): ApiVersionRes
}
