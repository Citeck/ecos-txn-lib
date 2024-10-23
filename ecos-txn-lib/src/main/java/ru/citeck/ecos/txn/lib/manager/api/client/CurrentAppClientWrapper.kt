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

    private val currAppName: String
    private val currAppRef: String

    init {
        val appProps = manager.webAppApi.getProperties()
        currAppName = appProps.appName
        currAppRef = currAppName + ":" + appProps.appInstanceId
    }

    private fun isCurrentApp(app: String): Boolean {
        return app == currAppName || app == currAppRef
    }

    override fun rollback(app: String, txnId: TxnId, cause: Throwable?) {
        if (isCurrentApp(app)) {
            manager.getManagedTransaction(txnId).rollback(cause)
        } else {
            impl.rollback(app, txnId, cause)
        }
    }

    override fun coordinateCommit(app: String, txnId: TxnId, data: TxnCommitData, txnLevel: Int) {
        if (isCurrentApp(app)) {
            error("Commit coordination can't be called for current app")
        }
        impl.coordinateCommit(app, txnId, data, txnLevel)
    }

    override fun onePhaseCommit(app: String, txnId: TxnId) {
        if (isCurrentApp(app)) {
            manager.getManagedTransaction(txnId).onePhaseCommit()
        } else {
            impl.onePhaseCommit(app, txnId)
        }
    }

    override fun disposeTxn(app: String, txnId: TxnId) {
        if (isCurrentApp(app)) {
            manager.dispose(txnId)
        } else {
            impl.disposeTxn(app, txnId)
        }
    }

    override fun prepareCommit(app: String, txnId: TxnId): List<EcosXid> {
        return if (isCurrentApp(app)) {
            manager.prepareCommitFromExtManager(txnId, true)
        } else {
            impl.prepareCommit(app, txnId)
        }
    }

    override fun commitPrepared(app: String, txnId: TxnId) {
        if (isCurrentApp(app)) {
            manager.getManagedTransaction(txnId).commitPrepared()
        } else {
            impl.commitPrepared(app, txnId)
        }
    }

    override fun recoveryCommit(app: String, txnId: TxnId, xids: Set<EcosXid>) {
        if (isCurrentApp(app)) {
            manager.recoveryCommit(txnId, xids)
        } else {
            impl.recoveryCommit(app, txnId, xids)
        }
    }

    override fun recoveryRollback(app: String, txnId: TxnId, xids: Set<EcosXid>) {
        if (isCurrentApp(app)) {
            manager.recoveryRollback(txnId, xids)
        } else {
            impl.recoveryRollback(app, txnId, xids)
        }
    }

    override fun getTxnStatus(app: String, txnId: TxnId): TransactionStatus {
        return if (isCurrentApp(app)) {
            manager.getTransactionOrNull(txnId)?.getStatus() ?: TransactionStatus.NO_TRANSACTION
        } else {
            impl.getTxnStatus(app, txnId)
        }
    }

    override fun executeTxnAction(app: String, txnId: TxnId, actionId: Int) {
        if (isCurrentApp(app)) {
            manager.getManagedTransaction(txnId).executeAction(actionId)
        } else {
            impl.executeTxnAction(app, txnId, actionId)
        }
    }

    override fun isApiVersionSupported(app: String, version: Int): ApiVersionRes {
        if (isCurrentApp(app)) {
            return ApiVersionRes.SUPPORTED
        }
        return impl.isApiVersionSupported(app, version)
    }

    override fun isAppAvailable(app: String): Boolean {
        if (isCurrentApp(app)) {
            return true
        }
        return impl.isAppAvailable(app)
    }
}
