package ru.citeck.ecos.txn.lib.support

import ru.citeck.ecos.txn.lib.commit.TxnCommitData
import ru.citeck.ecos.txn.lib.manager.TransactionManagerImpl
import ru.citeck.ecos.txn.lib.manager.api.client.ApiVersionRes
import ru.citeck.ecos.txn.lib.manager.api.client.TxnManagerRemoteApiClient
import ru.citeck.ecos.txn.lib.transaction.TxnId
import ru.citeck.ecos.txn.lib.transaction.xid.EcosXid

/**
 * Routes remote manager calls to the managers of [registry] in the same JVM, by app name or by
 * "appName:instanceId" of the manager's web app.
 */
internal class RoutingRemoteApiClient(
    private val registry: Map<String, TransactionManagerImpl>
) : TxnManagerRemoteApiClient {
    override fun prepareCommit(app: String, txnId: TxnId) =
        getManager(app).prepareCommitFromExtManager(txnId, true)
    override fun commitPrepared(app: String, txnId: TxnId) =
        getManager(app).getManagedTransaction(txnId).commitPrepared()
    override fun onePhaseCommit(app: String, txnId: TxnId) =
        getManager(app).getManagedTransaction(txnId).onePhaseCommit()
    override fun rollback(app: String, txnId: TxnId, cause: Throwable?) =
        getManager(app).getManagedTransaction(txnId).rollback(cause)
    override fun disposeTxn(app: String, txnId: TxnId) =
        getManager(app).dispose(txnId)
    override fun coordinateCommit(app: String, txnId: TxnId, data: TxnCommitData, txnLevel: Int) =
        getManager(app).coordinateCommit(txnId, data, txnLevel)
    override fun recoveryCommit(app: String, txnId: TxnId, xids: Set<EcosXid>) =
        getManager(app).recoveryCommit(txnId, xids)
    override fun recoveryRollback(app: String, txnId: TxnId, xids: Set<EcosXid>) =
        getManager(app).recoveryRollback(txnId, xids)
    override fun getTxnStatus(app: String, txnId: TxnId) =
        getManager(app).getStatus(txnId)
    override fun executeTxnAction(app: String, txnId: TxnId, actionId: Int) =
        getManager(app).getManagedTransaction(txnId).executeAction(actionId)
    override fun isApiVersionSupported(app: String, version: Int) =
        if (resolveManager(app) != null) ApiVersionRes.SUPPORTED else ApiVersionRes.APP_NOT_AVAILABLE
    override fun isAppAvailable(app: String) = resolveManager(app) != null
    private fun getManager(app: String) =
        resolveManager(app) ?: error("No manager for '$app'")
    private fun resolveManager(app: String): TransactionManagerImpl? {
        return registry[app] ?: registry.values.firstOrNull {
            val props = it.webAppApi.getProperties()
            app == "${props.appName}:${props.appInstanceId}"
        }
    }
}
