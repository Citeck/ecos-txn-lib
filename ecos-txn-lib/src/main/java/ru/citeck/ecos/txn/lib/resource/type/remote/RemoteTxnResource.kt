package ru.citeck.ecos.txn.lib.resource.type.remote

import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.txn.lib.manager.api.TxnManagerWebExecutor
import ru.citeck.ecos.txn.lib.resource.CommitPrepareStatus
import ru.citeck.ecos.txn.lib.resource.TransactionResource
import ru.citeck.ecos.txn.lib.transaction.TxnId
import ru.citeck.ecos.webapp.api.web.client.EcosWebClientApi
import ru.citeck.ecos.webapp.api.web.client.EcosWebClientResp

class RemoteTxnResource(
    private val appName: String,
    private val webClient: EcosWebClientApi,
    private val txnId: TxnId
) : TransactionResource {

    fun getAppName(): String {
        return appName
    }

    override fun start() {
        // should be already started
    }

    override fun end() {
        // end will be called inside remote transaction
    }

    override fun getName(): String {
        return "remote-txn-$appName"
    }

    override fun prepareCommit(): CommitPrepareStatus {
        return execRequest(TxnManagerWebExecutor.TYPE_PREPARE_COMMIT) {
            it.getBodyReader().readDto(TxnManagerWebExecutor.PrepareCommitResp::class.java).status
        }
    }

    override fun commitPrepared() {
        execRequest(TxnManagerWebExecutor.TYPE_COMMIT_PREPARED) {}
    }

    override fun onePhaseCommit() {
        execRequest(TxnManagerWebExecutor.TYPE_ONE_PHASE_COMMIT) {}
    }

    override fun rollback() {
        execRequest(TxnManagerWebExecutor.TYPE_ROLLBACK) {}
    }

    override fun dispose() {
        execRequest(TxnManagerWebExecutor.TYPE_DISPOSE) {}
    }

    private fun <T> execRequest(type: String, result: (EcosWebClientResp) -> T): T {
        return AuthContext.runAsSystem {
            webClient.newRequest()
                .targetApp(appName)
                .path(TxnManagerWebExecutor.PATH)
                .header(TxnManagerWebExecutor.HEADER_TYPE, type)
                .header(TxnManagerWebExecutor.HEADER_TXN_ID, txnId)
                .execute(result).get()
        }
    }
}
