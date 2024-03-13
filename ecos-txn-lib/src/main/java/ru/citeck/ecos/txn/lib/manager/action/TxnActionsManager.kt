package ru.citeck.ecos.txn.lib.manager.action

import mu.KotlinLogging
import ru.citeck.ecos.commons.promise.Promises
import ru.citeck.ecos.txn.lib.action.TxnActionId
import ru.citeck.ecos.txn.lib.action.TxnActionType
import ru.citeck.ecos.txn.lib.manager.TransactionManagerImpl
import ru.citeck.ecos.txn.lib.manager.action.obs.TxnActionsObsContext
import ru.citeck.ecos.txn.lib.transaction.TxnId
import ru.citeck.ecos.webapp.api.promise.Promise
import java.util.concurrent.CompletableFuture
import kotlin.system.measureTimeMillis

class TxnActionsManager(
    val manager: TransactionManagerImpl
) {

    companion object {
        private val log = KotlinLogging.logger {}
    }

    private val remoteClient = manager.remoteClient
    private val txnActionsExecutor = manager.webAppApi.getTasksApi().getExecutor("txn-actions")

    fun executeActionsAfterCommit(txnId: TxnId, txnLevel: Int, actions: List<TxnActionId>?): Promise<Unit> {
        if (actions.isNullOrEmpty()) {
            return Promises.resolve(Unit)
        }
        return runActionsInTaskExecutor(txnId, TxnActionType.AFTER_COMMIT) {
            executeActions(txnId, TxnActionType.AFTER_COMMIT, actions) { actionId ->
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

    fun executeActionsAfterRollback(
        txnId: TxnId,
        txnLevel: Int,
        mainError: Throwable,
        actions: List<TxnActionId>?
    ) {
        // after rollback actions usually used to revert invalid state
        // and should be executed in caller thread.
        // runActionsInTaskExecutor should not be used
        executeActions(txnId, TxnActionType.AFTER_ROLLBACK, actions) { actionId ->
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

    internal inline fun executeActions(
        txnId: TxnId,
        type: TxnActionType,
        actions: List<TxnActionId>?,
        crossinline execAction: (TxnActionId) -> Unit
    ) {
        if (actions.isNullOrEmpty()) {
            return
        }
        val startTime = System.currentTimeMillis()

        val obsCtx = TxnActionsObsContext(txnId, type, manager, actions)
        val actionsTime = HashMap<TxnActionId, Long>()
        manager.micrometerContext.createObs(obsCtx).observe {
            actions.forEach {
                actionsTime[it] = measureTimeMillis {
                    execAction.invoke(it)
                }
            }
            obsCtx.actionsTime = actionsTime
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

    private inline fun runActionsInTaskExecutor(
        txnId: TxnId,
        type: TxnActionType,
        crossinline action: () -> Unit
    ): Promise<Unit> {
        val future = CompletableFuture<Unit>()
        val data = manager.micrometerContext.extractScopeData()
        txnActionsExecutor.execute("$txnId-${type.observationId}-actions") {
            manager.micrometerContext.doWithinExtScope(data) {
                try {
                    action.invoke()
                    future.complete(Unit)
                } catch (e: Throwable) {
                    future.completeExceptionally(e)
                }
            }
        }
        return Promises.create(future)
    }

    private inline fun debug(txnId: TxnId, crossinline message: () -> String) {
        log.debug { "[$txnId] " + message() }
    }
}
