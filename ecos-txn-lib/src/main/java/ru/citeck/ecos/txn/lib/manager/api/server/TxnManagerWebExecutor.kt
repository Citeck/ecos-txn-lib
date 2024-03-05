package ru.citeck.ecos.txn.lib.manager.api.server

import ru.citeck.ecos.commons.data.DataValue
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.txn.lib.commit.TxnCommitData
import ru.citeck.ecos.txn.lib.manager.api.client.TxnManagerRemoteApiClient
import ru.citeck.ecos.txn.lib.manager.api.server.action.*
import ru.citeck.ecos.txn.lib.transaction.TxnId
import ru.citeck.ecos.webapp.api.web.executor.EcosWebExecutor
import ru.citeck.ecos.webapp.api.web.executor.EcosWebExecutorReq
import ru.citeck.ecos.webapp.api.web.executor.EcosWebExecutorResp

class TxnManagerWebExecutor constructor(
    private val remoteActions: TxnManagerRemoteActions
) : EcosWebExecutor {

    companion object {

        const val PATH = "/txn"

        const val HEADER_TYPE = "type"

        const val V0_HEADER_TXN_ID = "txnId"
        const val V0_HEADER_ACTION_ID = "actionId"

        const val V0_TYPE_ONE_PHASE_COMMIT = "one-phase-commit"
        const val V0_TYPE_PREPARE_COMMIT = "prepare-commit"
        const val V0_TYPE_COMMIT_PREPARED = "commit-prepared"
        const val V0_TYPE_DISPOSE = "dispose"
        const val V0_TYPE_ROLLBACK = "rollback"
        const val V0_TYPE_EXEC_ACTION = "exec-action"
        const val V0_TYPE_GET_STATUS = "get-status"
        const val V0_TYPE_COORDINATE_COMMIT = "coordinate-commit"
    }

    override fun execute(request: EcosWebExecutorReq, response: EcosWebExecutorResp) {

        if (!AuthContext.isRunAsSystem()) {
            error("Permission denied")
        }

        if (request.getApiVersion() <= 1) {
            executeV0(request, response)
        } else {
            val type = request.getHeaders().get("type") ?: ""
            if (type.isBlank()) {
                error("header 'type' is not defined")
            }
            val data = request.getBodyReader().readDto(DataValue::class.java)
            val respData = remoteActions.execute(type, data, request.getApiVersion())
            if (respData != null) {
                response.getBodyWriter().writeDto(respData)
            }
        }
    }

    private fun executeV0(request: EcosWebExecutorReq, response: EcosWebExecutorResp) {

        val headers = request.getHeaders()

        val type = headers.get(HEADER_TYPE) ?: error("Type is not defined")
        val txnId = TxnId.valueOf(headers.get(V0_HEADER_TXN_ID) ?: "")
        if (txnId.isEmpty()) {
            error("$V0_HEADER_TXN_ID is not defined")
        }

        val body = DataValue.createObj()
        body["txnId"] = txnId.toString()

        val actionType = when (type) {
            V0_TYPE_ONE_PHASE_COMMIT -> TmOnePhaseCommitAction.TYPE
            V0_TYPE_PREPARE_COMMIT -> TmPrepareCommitAction.TYPE
            V0_TYPE_COMMIT_PREPARED -> TmCommitPreparedAction.TYPE
            V0_TYPE_DISPOSE -> TmDisposeTxnAction.TYPE
            V0_TYPE_ROLLBACK -> TmRollbackTxnAction.TYPE
            V0_TYPE_GET_STATUS -> TmGetStatusAction.TYPE
            V0_TYPE_EXEC_ACTION -> {
                val actionId = headers.get(V0_HEADER_ACTION_ID, Int::class.java)
                    ?: error("Header $V0_HEADER_ACTION_ID is not defined")
                body["actionId"] = actionId
                TmExecActionAction.TYPE
            }
            V0_TYPE_COORDINATE_COMMIT -> {
                val readDto = request.getBodyReader().readDto(TxnCommitReqBody::class.java)
                body["data"] = readDto.data
                body["txnLevel"] = readDto.txnLevel
                TmCoordinateCommitAction.TYPE
            }
            else -> error("Unknown type $type")
        }

        val respData = remoteActions.execute(actionType, body, 0)
        if (respData != null) {
            response.getBodyWriter().writeDto(respData)
        }
    }

    override fun getPath() = PATH
    override fun isReadOnly() = false

    override fun getApiVersion(): Pair<Int, Int> {
        return 0 to TxnManagerRemoteApiClient.CURRENT_API_VERSION
    }

    data class TxnCommitReqBody(
        val data: TxnCommitData,
        val txnLevel: Int
    )
}
