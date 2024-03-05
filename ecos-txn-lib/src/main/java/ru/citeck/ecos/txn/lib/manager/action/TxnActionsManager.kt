package ru.citeck.ecos.txn.lib.manager.action

import mu.KotlinLogging
import ru.citeck.ecos.txn.lib.action.TxnActionId
import ru.citeck.ecos.txn.lib.action.TxnActionType
import ru.citeck.ecos.txn.lib.manager.TransactionManagerImpl
import ru.citeck.ecos.txn.lib.transaction.TxnId

class TxnActionsManager(
    private val manager: TransactionManagerImpl
) {

    companion object {
        private val log = KotlinLogging.logger {}
    }

    private val remoteClient = manager.remoteClient

    fun executeActionsAfterCommit(txnId: TxnId, txnLevel: Int, actions: List<TxnActionId>?) {
        actions ?: return
        executeActionsImpl(txnId, TxnActionType.AFTER_COMMIT) {
            actions.forEach { actionId ->
                try {
                    manager.doInNewTxn(false, txnLevel + 1) {
                        executeActionById(txnId, TxnActionType.AFTER_COMMIT, actionId)
                    }
                } catch (mainError: Throwable) {
                    log.error(mainError) {
                        "After commit action execution error. Id: $actionId"
                    }
                }
            }
        }
    }

    fun executeActionsAfterRollback(txnId: TxnId, txnLevel: Int, mainError: Throwable, actions: List<TxnActionId>?) {
        actions ?: return
        executeActionsImpl(txnId, TxnActionType.AFTER_ROLLBACK) {
            actions.forEach { actionId ->
                try {
                    manager.doInNewTxn(false, txnLevel + 1) {
                        executeActionById(txnId, TxnActionType.AFTER_ROLLBACK, actionId)
                    }
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

        remoteClient.executeTxnAction(actionId.appName, txnId, actionId.localId)

        val executedTime = System.currentTimeMillis() - actionStartTime
        debug(txnId) { "Action $type with id: $actionId, executed in $executedTime ms" }
    }

    private inline fun debug(txnId: TxnId, crossinline message: () -> String) {
        log.debug { "[$txnId] " + message() }
    }
}
