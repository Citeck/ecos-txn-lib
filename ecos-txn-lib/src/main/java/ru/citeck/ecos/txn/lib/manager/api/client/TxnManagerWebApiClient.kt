package ru.citeck.ecos.txn.lib.manager.api.client

import io.github.oshai.kotlinlogging.KotlinLogging
import ru.citeck.ecos.txn.lib.TxnContext
import ru.citeck.ecos.txn.lib.commit.TxnCommitData
import ru.citeck.ecos.txn.lib.manager.api.server.TxnManagerWebExecutor
import ru.citeck.ecos.txn.lib.manager.api.server.action.*
import ru.citeck.ecos.txn.lib.resource.CommitPrepareStatus
import ru.citeck.ecos.txn.lib.transaction.TransactionStatus
import ru.citeck.ecos.txn.lib.transaction.TxnId
import ru.citeck.ecos.txn.lib.transaction.xid.EcosXid
import ru.citeck.ecos.webapp.api.apps.EcosRemoteWebAppsApi
import ru.citeck.ecos.webapp.api.web.client.EcosWebClientApi
import ru.citeck.ecos.webapp.api.web.client.EcosWebClientResp

class TxnManagerWebApiClient(
    private val webClient: EcosWebClientApi,
    private val remoteWebAppsApi: EcosRemoteWebAppsApi
) : TxnManagerRemoteApiClient {

    companion object {
        const val HEADER_TYPE = "type"

        private const val V0_HEADER_TXN_ID = "txnId"
        private const val V0_HEADER_ACTION_ID = "actionId"

        private val log = KotlinLogging.logger {}
    }

    override fun coordinateCommit(app: String, txnId: TxnId, data: TxnCommitData, txnLevel: Int) {
        val body = TmCoordinateCommitAction.Data(txnId, data, txnLevel)
        execRequest(app, TmCoordinateCommitAction.TYPE, body) { _, _ -> }
    }

    override fun recoveryCommit(app: String, txnId: TxnId, xids: Set<EcosXid>) {
        val body = TmRecoveryCommitAction.Data(txnId, ArrayList(xids))
        execRequest(app, TmRecoveryCommitAction.TYPE, body) { _, _ -> }
    }

    override fun recoveryRollback(app: String, txnId: TxnId, xids: Set<EcosXid>) {
        val body = TmRecoveryRollbackAction.Data(txnId, ArrayList(xids))
        execRequest(app, TmRecoveryRollbackAction.TYPE, body) { _, _ -> }
    }

    override fun disposeTxn(app: String, txnId: TxnId) {
        val body = TmDisposeTxnAction.Data(txnId)
        execRequest(app, TmDisposeTxnAction.TYPE, body) { _, _ -> }
    }

    override fun onePhaseCommit(app: String, txnId: TxnId) {
        val body = TmOnePhaseCommitAction.Data(txnId)
        execRequest(app, TmOnePhaseCommitAction.TYPE, body) { _, _ -> }
    }

    override fun prepareCommit(app: String, txnId: TxnId): List<EcosXid> {
        val body = TmPrepareCommitAction.Data(txnId)
        return execRequest(app, TmPrepareCommitAction.TYPE, body) { ver, res ->
            if (ver <= 1) {
                val status = res.getBodyReader().readDto(TmPrepareCommitAction.RespV0::class.java).status
                if (status == CommitPrepareStatus.NOTHING_TO_COMMIT) {
                    emptyList()
                } else {
                    // stub for legacy api to emulate remote xids existence
                    listOf(EcosXid.create(txnId, ByteArray(0)))
                }
            } else {
                res.getBodyReader().readDto(TmPrepareCommitAction.Resp::class.java).preparedXids
            }
        }
    }

    override fun commitPrepared(app: String, txnId: TxnId) {
        val body = TmCommitPreparedAction.Data(txnId)
        execRequest(app, TmCommitPreparedAction.TYPE, body) { _, _ -> }
    }

    override fun executeTxnAction(app: String, txnId: TxnId, actionId: Int) {
        val body = TmExecActionAction.Data(txnId, actionId)
        execRequest(app, TmExecActionAction.TYPE, body) { _, _ -> }
    }

    override fun rollback(app: String, txnId: TxnId, cause: Throwable?) {
        val body = TmRollbackTxnAction.Data(txnId)
        if (cause == null) {
            execRequest(app, TmRollbackTxnAction.TYPE, body) { _, _ -> }
        } else {
            try {
                execRequest(app, TmRollbackTxnAction.TYPE, body) { _, _ -> }
            } catch (e: Throwable) {
                cause.addSuppressed(e)
            }
        }
    }

    override fun getTxnStatus(app: String, txnId: TxnId): TransactionStatus {
        val body = TmGetStatusAction.Data(txnId)
        return execRequest(app, TmGetStatusAction.TYPE, body) { _, res ->
            res.getBodyReader().readDto(TmGetStatusAction.GetStatusResp::class.java).status
        }
    }

    override fun isAppAvailable(app: String): Boolean {
        return remoteWebAppsApi.isAppAvailable(app)
    }

    override fun isApiVersionSupported(app: String, version: Int): ApiVersionRes {
        return when (webClient.getApiVersion(app, TxnManagerWebExecutor.PATH, version)) {
            EcosWebClientApi.AV_VERSION_NOT_SUPPORTED -> ApiVersionRes.NOT_SUPPORTED
            EcosWebClientApi.AV_APP_NOT_AVAILABLE,
            EcosWebClientApi.AV_PATH_NOT_SUPPORTED -> ApiVersionRes.APP_NOT_AVAILABLE

            version -> ApiVersionRes.SUPPORTED
            else -> ApiVersionRes.NOT_SUPPORTED
        }
    }

    private fun <T> execRequest(
        app: String,
        type: String,
        body: Any,
        result: (Int, EcosWebClientResp) -> T
    ): T {
        val targetVersion = webClient.getApiVersion(
            app,
            TxnManagerWebExecutor.PATH,
            TxnManagerRemoteApiClient.CURRENT_API_VERSION
        )
        log.debug {
            "[${TxnContext.getTxnOrNull()?.getId() ?: "no-txn"}]" +
                " Exec request to app '$app' type '$type' body '$body' apiVer: '$targetVersion'"
        }

        return if (targetVersion >= TxnManagerRemoteApiClient.COORDINATE_COMMIT_VER) {
            val req = webClient.newRequest()
                .targetApp(app)
                .version(targetVersion)
                .path(TxnManagerWebExecutor.PATH)
                .header(HEADER_TYPE, type)
                .body { it.writeDto(body) }
            req.executeSync { result.invoke(targetVersion, it) }
        } else {
            execV0Request(app, type, body) { result.invoke(0, it) }
        }
    }

    private fun <T> execV0Request(appName: String, type: String, body: Any, result: (EcosWebClientResp) -> T): T {

        val req = webClient.newRequest()
            .targetApp(appName)
            .path(TxnManagerWebExecutor.PATH)
            .header(HEADER_TYPE, type)

        when (type) {
            TmCommitPreparedAction.TYPE -> {
                val data = body as TmCommitPreparedAction.Data
                req.header(V0_HEADER_TXN_ID, data.txnId)
            }

            TmDisposeTxnAction.TYPE -> {
                val data = body as TmDisposeTxnAction.Data
                req.header(V0_HEADER_TXN_ID, data.txnId)
            }

            TmExecActionAction.TYPE -> {
                val data = body as TmExecActionAction.Data
                req.header(V0_HEADER_TXN_ID, data.txnId)
                req.header(V0_HEADER_ACTION_ID, data.actionId)
            }

            TmGetStatusAction.TYPE -> {
                val data = body as TmGetStatusAction.Data
                req.header(V0_HEADER_TXN_ID, data.txnId)
            }

            TmOnePhaseCommitAction.TYPE -> {
                val data = body as TmOnePhaseCommitAction.Data
                req.header(V0_HEADER_TXN_ID, data.txnId)
            }

            TmPrepareCommitAction.TYPE -> {
                val data = body as TmPrepareCommitAction.Data
                req.header(V0_HEADER_TXN_ID, data.txnId)
            }

            TmRollbackTxnAction.TYPE -> {
                val data = body as TmRollbackTxnAction.Data
                req.header(V0_HEADER_TXN_ID, data.txnId)
            }

            else -> {
                error("Unsupported action type: $type")
            }
        }
        return req.executeSync(result)
    }
}
