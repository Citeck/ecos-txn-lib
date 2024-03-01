package ru.citeck.ecos.txn.lib.manager.api

import mu.KotlinLogging
import ru.citeck.ecos.commons.data.DataValue
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.micrometer.EcosMicrometerContext
import ru.citeck.ecos.txn.lib.commit.TxnCommitData
import ru.citeck.ecos.txn.lib.manager.TransactionManager
import ru.citeck.ecos.txn.lib.resource.CommitPrepareStatus
import ru.citeck.ecos.txn.lib.transaction.TransactionStatus
import ru.citeck.ecos.txn.lib.transaction.TxnId
import ru.citeck.ecos.txn.lib.transaction.xid.EcosXid
import ru.citeck.ecos.webapp.api.web.EcosWebHeaders
import ru.citeck.ecos.webapp.api.web.executor.EcosWebExecutor
import ru.citeck.ecos.webapp.api.web.executor.EcosWebExecutorReq
import ru.citeck.ecos.webapp.api.web.executor.EcosWebExecutorResp

class TxnManagerWebExecutor @JvmOverloads constructor(
    private val manager: TransactionManager,
    private val micrometerContext: EcosMicrometerContext = EcosMicrometerContext.NOOP
) : EcosWebExecutor {

    companion object {

        const val VERSION = 1

        const val PATH = "/txn"

        const val HEADER_TYPE = "type"
        const val HEADER_TXN_ID = "txnId"
        const val HEADER_ACTION_ID = "actionId"

        const val HEADER_XIDS = "xids"

        const val TYPE_ONE_PHASE_COMMIT = "one-phase-commit"
        const val TYPE_PREPARE_COMMIT = "prepare-commit"
        const val TYPE_COMMIT_PREPARED = "commit-prepared"
        const val TYPE_DISPOSE = "dispose"
        const val TYPE_ROLLBACK = "rollback"
        const val TYPE_EXEC_ACTION = "exec-action"
        const val TYPE_GET_STATUS = "get-status"

        const val TYPE_COORDINATE_COMMIT = "coordinate-commit"

        const val TYPE_RECOVERY_COMMIT = "recovery-commit"
        const val TYPE_RECOVERY_ROLLBACK = "recovery-rollback"

        private val log = KotlinLogging.logger {}
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

        val observation = micrometerContext.createObservation("ecos.txn.external-action")
            .highCardinalityKeyValue("txnId", txnId.toString())
            .highCardinalityKeyValue("type", type)

        if (type == TYPE_EXEC_ACTION) {
            observation.highCardinalityKeyValue(
                HEADER_ACTION_ID,
                headers.get(HEADER_ACTION_ID) ?: ""
            )
        } else if (type == TYPE_RECOVERY_COMMIT || type == TYPE_RECOVERY_ROLLBACK) {
            observation.highCardinalityKeyValue(
                HEADER_XIDS,
                headers.get(HEADER_XIDS) ?: ""
            )
        }

        observation.observe {
            when (type) {
                TYPE_ONE_PHASE_COMMIT -> {
                    manager.getManagedTransaction(txnId).onePhaseCommit()
                }
                TYPE_PREPARE_COMMIT -> {
                    val status = manager.prepareCommitFromExtManager(
                        txnId,
                        request.getApiVersion() > 0 // recovering appeared at ver.1
                    )
                    response.getBodyWriter().writeDto(PrepareCommitResp(status))
                }
                TYPE_COMMIT_PREPARED -> manager.getManagedTransaction(txnId).commitPrepared()
                TYPE_DISPOSE -> manager.dispose(txnId)
                TYPE_ROLLBACK -> manager.getManagedTransaction(txnId).rollback(null)
                TYPE_EXEC_ACTION -> {
                    val actionId = headers.get(HEADER_ACTION_ID, Int::class.java)
                        ?: error("Header $HEADER_ACTION_ID is not defined")
                    manager.getManagedTransaction(txnId).executeAction(actionId)
                }
                TYPE_GET_STATUS -> {
                    response.getBodyWriter().writeDto(GetStatusResp(manager.getStatus(txnId)))
                }
                TYPE_COORDINATE_COMMIT -> {
                    val readDto = request.getBodyReader().readDto(TxnCommitReqBody::class.java)
                    manager.coordinateCommit(txnId, readDto.data, readDto.txnLevel)
                }
                TYPE_RECOVERY_COMMIT -> {
                    val txn = manager.getManagedTransactionOrNull(txnId)
                    if (txn != null) {
                        txn.commitPrepared()
                    } else {
                        val xids = readXidsFromHeaders(headers)
                        manager.getRecoveryManager().commitPrepared(xids)
                    }
                }
                TYPE_RECOVERY_ROLLBACK -> {
                    val txn = manager.getManagedTransactionOrNull(txnId)
                    if (txn != null) {
                        txn.rollback(null)
                    } else {
                        val xids = readXidsFromHeaders(headers)
                        manager.getRecoveryManager().rollbackPrepared(xids)
                    }
                }
                else -> error("Unknown type $type")
            }
        }
    }

    private fun readXidsFromHeaders(headers: EcosWebHeaders): List<EcosXid> {
        val xids = headers.get(HEADER_XIDS, DataValue::class.java)
            ?: error("Header $HEADER_XIDS is not defined")
        return xids.asList(EcosXid::class.java)
    }

    override fun getPath() = PATH
    override fun isReadOnly() = false

    override fun getApiVersion(): Pair<Int, Int> {
        return 0 to VERSION
    }

    data class PrepareCommitResp(
        val status: CommitPrepareStatus
    )

    data class GetStatusResp(
        val status: TransactionStatus
    )

    data class TxnCommitReqBody(
        val data: TxnCommitData,
        val txnLevel: Int
    )
}
