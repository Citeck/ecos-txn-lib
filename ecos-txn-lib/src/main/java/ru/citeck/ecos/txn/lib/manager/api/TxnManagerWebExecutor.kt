package ru.citeck.ecos.txn.lib.manager.api

import io.micrometer.observation.Observation
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.micrometer.EcosMicrometerContext
import ru.citeck.ecos.micrometer.observeKt
import ru.citeck.ecos.txn.lib.manager.TransactionManager
import ru.citeck.ecos.txn.lib.resource.CommitPrepareStatus
import ru.citeck.ecos.txn.lib.transaction.TransactionStatus
import ru.citeck.ecos.txn.lib.transaction.TxnId
import ru.citeck.ecos.webapp.api.web.executor.EcosWebExecutor
import ru.citeck.ecos.webapp.api.web.executor.EcosWebExecutorReq
import ru.citeck.ecos.webapp.api.web.executor.EcosWebExecutorResp

class TxnManagerWebExecutor @JvmOverloads constructor(
    private val manager: TransactionManager,
    private val micrometerContext: EcosMicrometerContext = EcosMicrometerContext.NOOP
) : EcosWebExecutor {

    companion object {
        const val PATH = "/txn"

        const val HEADER_TYPE = "type"
        const val HEADER_TXN_ID = "txnId"
        const val HEADER_ACTION_ID = "actionId"

        const val TYPE_ONE_PHASE_COMMIT = "one-phase-commit"
        const val TYPE_PREPARE_COMMIT = "prepare-commit"
        const val TYPE_COMMIT_PREPARED = "commit-prepared"
        const val TYPE_DISPOSE = "dispose"
        const val TYPE_ROLLBACK = "rollback"
        const val TYPE_EXEC_ACTION = "exec-action"
        const val TYPE_GET_STATUS = "get-status"
    }

    override fun execute(request: EcosWebExecutorReq, response: EcosWebExecutorResp) {

        if (!AuthContext.isRunAsSystem()) {
            error("Permission denied")
        }

        val headers = request.getHeaders()

        val type = headers.get(HEADER_TYPE) ?: error("Type is not defined")
        val txnId = TxnId.valueOf(headers.get(HEADER_TXN_ID) ?: "")
        if (txnId.isEmpty()) {
            error("$HEADER_TXN_ID is not defined")
        }

        val observation = Observation.createNotStarted(
            "ecos.txn.external-action",
            micrometerContext.getObservationRegistry()
        ).highCardinalityKeyValue("txnId", txnId.toString())
            .highCardinalityKeyValue("type", type)

        if (!observation.isNoop && type == TYPE_EXEC_ACTION) {
            observation.highCardinalityKeyValue(
                HEADER_ACTION_ID,
                headers.get(HEADER_ACTION_ID, Int::class.java).toString()
            )
        }

        observation.observeKt {
            when (type) {
                TYPE_ONE_PHASE_COMMIT -> {
                    manager.onePhaseCommit(txnId)
                }
                TYPE_PREPARE_COMMIT -> {
                    val status = manager.prepareCommit(txnId)
                    response.getBodyWriter().writeDto(PrepareCommitResp(status))
                }
                TYPE_COMMIT_PREPARED -> manager.commitPrepared(txnId)
                TYPE_DISPOSE -> manager.dispose(txnId)
                TYPE_ROLLBACK -> manager.rollback(txnId)
                TYPE_EXEC_ACTION -> {
                    val actionId = headers.get(HEADER_ACTION_ID, Int::class.java)
                        ?: error("Header $HEADER_ACTION_ID is not defined")
                    manager.executeAction(txnId, actionId)
                }
                TYPE_GET_STATUS -> {
                    response.getBodyWriter().writeDto(GetStatusResp(manager.getStatus(txnId)))
                }
            }
        }
    }

    override fun getPath() = PATH
    override fun isReadOnly() = false

    data class PrepareCommitResp(
        val status: CommitPrepareStatus
    )

    data class GetStatusResp(
        val status: TransactionStatus
    )
}
