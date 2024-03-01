package ru.citeck.ecos.txn.lib.manager.action

import mu.KotlinLogging
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.txn.lib.action.TxnActionId
import ru.citeck.ecos.txn.lib.action.TxnActionType
import ru.citeck.ecos.txn.lib.manager.TransactionManagerImpl
import ru.citeck.ecos.txn.lib.manager.api.TxnManagerWebExecutor
import ru.citeck.ecos.txn.lib.transaction.TxnId

class TxnActionsManager(
    private val manager: TransactionManagerImpl
) {

    companion object {
        private val log = KotlinLogging.logger {}
    }

    private val webClientApi = manager.webAppApi.getWebClientApi()
    private val properties = manager.webAppApi.getProperties()

    fun executeActionsAfterCommit(txnId: TxnId, txnLevel: Int, actions: List<TxnActionId>?) {
        actions ?: return
        executeActionsImpl(txnId, TxnActionType.AFTER_COMMIT) {
            actions.forEach { actionId ->
                manager.doInNewTxn(false, txnLevel + 1) {
                    try {
                        executeActionById(txnId, TxnActionType.AFTER_COMMIT, actionId)
                    } catch (mainError: Throwable) {
                        log.error(mainError) {
                            "After commit action execution error. Id: $actionId"
                        }
                    }
                }
            }
        }
    }

    fun executeActionsAfterRollback(txnId: TxnId, txnLevel: Int, mainError: Throwable, actions: List<TxnActionId>?) {
        actions ?: return
        executeActionsImpl(txnId, TxnActionType.AFTER_ROLLBACK) {
            actions.forEach { actionId ->
                manager.doInNewTxn(false, txnLevel + 1) {
                    try {
                        executeActionById(txnId, TxnActionType.AFTER_ROLLBACK, actionId)
                    } catch (afterRollbackActionErr: Throwable) {
                        mainError.addSuppressed(
                            RuntimeException(
                                "[$txnId] After rollback action execution error. Id: $actionId",
                                afterRollbackActionErr
                            )
                        )
                    }
                }
            }
        }
    }

    private inline fun executeActionsImpl(txnId: TxnId, type: TxnActionType, crossinline executeActions: () -> Unit) {

        val startTime = System.currentTimeMillis()

        val observation = manager.micrometerContext.createObservation(type.observationId)
            .highCardinalityKeyValue("txnId") { txnId.toString() }

        observation.observe {
            executeActions.invoke()
        }

        val totalTime = System.currentTimeMillis() - startTime

        debug(txnId) { "Actions $type total time: $totalTime ms" }
    }

    fun executeActionById(txnId: TxnId, type: TxnActionType, actionId: TxnActionId) {

        debug(txnId) { "Execute $type action with id: $actionId" }

        val actionStartTime = System.currentTimeMillis()

        if (actionId.appName == properties.appName) {
            manager.getManagedTransaction(txnId).executeAction(actionId.localId)
        } else {
            AuthContext.runAsSystem {
                webClientApi.newRequest()
                    .targetApp(actionId.appName)
                    .path(TxnManagerWebExecutor.PATH)
                    .header(TxnManagerWebExecutor.HEADER_TXN_ID, txnId)
                    .header(TxnManagerWebExecutor.HEADER_TYPE, TxnManagerWebExecutor.TYPE_EXEC_ACTION)
                    .header(TxnManagerWebExecutor.HEADER_ACTION_ID, actionId.localId)
                    .execute {}.get()
            }
        }

        val executedTime = System.currentTimeMillis() - actionStartTime
        debug(txnId) { "Action $type with id: $actionId, executed in $executedTime ms" }
    }

    private inline fun debug(txnId: TxnId, crossinline message: () -> String) {
        log.debug { "[$txnId] " + message() }
    }
}
