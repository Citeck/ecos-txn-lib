package ru.citeck.ecos.txn.lib.manager.api.client

import ru.citeck.ecos.txn.lib.commit.TxnCommitData
import ru.citeck.ecos.txn.lib.manager.TransactionManagerImpl
import ru.citeck.ecos.txn.lib.transaction.TransactionStatus
import ru.citeck.ecos.txn.lib.transaction.TxnId
import ru.citeck.ecos.txn.lib.transaction.xid.EcosXid

class CurrentAppClientWrapper(
    private val impl: TxnManagerRemoteApiClient,
    private val manager: TransactionManagerImpl
) : TxnManagerRemoteApiClient {

    private val currentApp = manager.webAppApi.getProperties().appName

    override fun rollback(app: String, txnId: TxnId, cause: Throwable?) {
        if (app == currentApp) {
            manager.getManagedTransaction(txnId).rollback(cause)
        } else {
            impl.rollback(app, txnId, cause)
        }
    }

    override fun coordinateCommit(app: String, txnId: TxnId, data: TxnCommitData, txnLevel: Int) {
        if (app == currentApp) {
            error("Commit coordination can't be called for current app")
        }
        impl.coordinateCommit(app, txnId, data, txnLevel)
    }

    override fun onePhaseCommit(app: String, txnId: TxnId) {
        if (app == currentApp) {
            manager.getManagedTransaction(txnId).onePhaseCommit()
        } else {
            impl.onePhaseCommit(app, txnId)
        }
    }

    override fun disposeTxn(app: String, txnId: TxnId) {
        if (app == currentApp) {
            manager.dispose(txnId)
        } else {
            impl.disposeTxn(app, txnId)
        }
    }

    override fun prepareCommit(app: String, txnId: TxnId): List<EcosXid> {
        return if (app == currentApp) {
            manager.prepareCommitFromExtManager(txnId, true)
        } else {
            impl.prepareCommit(app, txnId)
        }
    }

    override fun commitPrepared(app: String, txnId: TxnId) {
        if (app == currentApp) {
            manager.getManagedTransaction(txnId).commitPrepared()
        } else {
            impl.commitPrepared(app, txnId)
        }
    }

    override fun recoveryCommit(app: String, txnId: TxnId, xids: Set<EcosXid>) {
        if (app == currentApp) {
            manager.recoveryCommit(txnId, xids)
        } else {
            impl.recoveryCommit(app, txnId, xids)
        }
    }

    override fun recoveryRollback(app: String, txnId: TxnId, xids: Set<EcosXid>) {
        if (app == currentApp) {
            manager.recoveryRollback(txnId, xids)
        } else {
            impl.recoveryRollback(app, txnId, xids)
        }
    }

    override fun getTxnStatus(app: String, txnId: TxnId): TransactionStatus {
        return if (app == currentApp) {
            manager.getTransactionOrNull(txnId)?.getStatus() ?: TransactionStatus.NO_TRANSACTION
        } else {
            impl.getTxnStatus(app, txnId)
        }
    }

    override fun executeTxnAction(app: String, txnId: TxnId, actionId: Int) {
        if (app == currentApp) {
            manager.getManagedTransaction(txnId).executeAction(actionId)
        } else {
            impl.executeTxnAction(app, txnId, actionId)
        }
    }

    override fun isApiVersionSupported(app: String, version: Int): ApiVersionRes {
        if (app == currentApp) {
            return ApiVersionRes.SUPPORTED
        }
        return impl.isApiVersionSupported(app, version)
    }

    override fun isAppAvailable(app: String): Boolean {
        if (app == currentApp) {
            return true
        }
        return impl.isAppAvailable(app)
    }
}
