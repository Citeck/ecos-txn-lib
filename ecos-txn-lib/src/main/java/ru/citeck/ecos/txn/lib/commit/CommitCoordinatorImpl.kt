package ru.citeck.ecos.txn.lib.commit

import mu.KotlinLogging
import ru.citeck.ecos.commons.json.Json
import ru.citeck.ecos.txn.lib.action.TxnActionId
import ru.citeck.ecos.txn.lib.action.TxnActionType
import ru.citeck.ecos.txn.lib.commit.repo.NoopTwoPhaseCommitRepo
import ru.citeck.ecos.txn.lib.commit.repo.TwoPhaseCommitRepo
import ru.citeck.ecos.txn.lib.commit.repo.TwoPhaseCommitStatus
import ru.citeck.ecos.txn.lib.manager.TransactionManagerImpl
import ru.citeck.ecos.txn.lib.manager.api.client.ApiVersionRes
import ru.citeck.ecos.txn.lib.manager.api.client.TxnManagerRemoteApiClient
import ru.citeck.ecos.txn.lib.transaction.TransactionStatus
import ru.citeck.ecos.txn.lib.transaction.TxnId
import java.util.concurrent.atomic.AtomicBoolean

class CommitCoordinatorImpl(
    private val repo: TwoPhaseCommitRepo,
    private val manager: TransactionManagerImpl
) : CommitCoordinator {

    companion object {
        private val log = KotlinLogging.logger {}
    }

    private val appLockApi = manager.webAppApi.getAppLockApi()
    private var currentApp: String = manager.webAppApi.getProperties().appName
    private val actionsManager = manager.actionsManager
    private val remoteClient = manager.remoteClient
    private val commitCoordinatorApp = manager.props.commitCoordinatorApp
    private val micrometerContext = manager.micrometerContext

    override fun commitRoot(txnId: TxnId, data: TxnCommitData, txnLevel: Int) {

        if (data.apps.isEmpty() && data.actions.isEmpty()) {
            manager.dispose(txnId)
            return
        }

        val commitObservation = micrometerContext.createObservation("ecos.txn.commit")
            .highCardinalityKeyValue("txnId") { txnId.toString() }
            .highCardinalityKeyValue("apps") { data.apps.keys.joinToString(",") }

        if (data.apps.size == 1) {
            val appEntry = data.apps.entries.first()
            if (appEntry.value.size == 1) {
                val appToCommit = appEntry.key
                commitObservation.observe {
                    remoteClient.onePhaseCommit(appToCommit, txnId)
                }
                actionsManager.executeActionsAfterCommit(txnId, txnLevel, data.actions[TxnActionType.AFTER_COMMIT])
                disposeRoot(txnId, setOf(appToCommit), null)
                return
            }
        }
        if (commitCoordinatorApp.isNotBlank() && commitCoordinatorApp != currentApp) {
            if (!isRemoteCommitCoordinatorAvailable()) {
                log.debug {
                    "Commit coordinator app $commitCoordinatorApp doesn't support required web-api path."
                }
            } else {
                remoteClient.coordinateCommit(commitCoordinatorApp, txnId, data, txnLevel)
                manager.dispose(txnId)
                return
            }
        }
        repo.beforePrepare(txnId, data)

        val txnApps = HashSet(data.apps.keys)
        data.actions.forEach { (k, v) -> v.forEach { txnApps.add(it.appName) } }

        val preparedAppsToCommit = HashSet<String>()
        for (app in txnApps) {
            try {
                if (data.apps.containsKey(app)) {
                    val xids = remoteClient.prepareCommit(app, txnId)
                    if (xids.isNotEmpty()) {
                        preparedAppsToCommit.add(app)
                    } else {
                        // if app answered 'NOTHING TO COMMIT', then rollback and disposing is not required
                        txnApps.remove(app)
                    }
                } else {
                    // app without txn resources
                    remoteClient.onePhaseCommit(app, txnId)
                }
            } catch (mainError: Throwable) {
                repo.beforeRollback(txnId, txnApps)
                rollbackRoot(
                    txnId,
                    txnApps,
                    data.actions[TxnActionType.AFTER_ROLLBACK],
                    mainError,
                    txnLevel
                )
                throw mainError
            }
        }

        repo.beforeCommit(txnId, preparedAppsToCommit)

        val commitErrors = HashMap<String, Throwable>()
        if (preparedAppsToCommit.isEmpty()) {
            repo.afterCommit(txnId, emptySet(), emptyMap())
        } else {
            val committedApps = HashSet<String>()
            var commitError: Throwable? = null
            for (appToCommit in preparedAppsToCommit) {
                try {
                    remoteClient.commitPrepared(appToCommit, txnId)
                    committedApps.add(appToCommit)
                } catch (e: Throwable) {
                    // If a commit attempt fails for any app, the failed transaction
                    // will be later recovered and the error handled accordingly.
                    // The first error is captured, and any subsequent errors
                    // are suppressed and associated with the initial one.
                    commitErrors[appToCommit] = e
                    if (commitError == null) {
                        commitError = e
                    } else {
                        commitError.addSuppressed(e)
                    }
                }
            }
            repo.afterCommit(txnId, committedApps, commitErrors)
            if (commitError != null) {
                throw commitError
            }
        }
        if (commitErrors.isEmpty()) {
            actionsManager.executeActionsAfterCommit(txnId, txnLevel, data.actions[TxnActionType.AFTER_COMMIT])
        }
        disposeRoot(txnId, txnApps, null)
    }

    override fun rollbackRoot(
        txnId: TxnId,
        apps: Collection<String>,
        actions: List<TxnActionId>?,
        error: Throwable,
        txnLevel: Int
    ) {

        if (commitCoordinatorApp.isNotBlank() && commitCoordinatorApp != currentApp) {
            val coordinatorTxnStatus = remoteClient.getTxnStatus(commitCoordinatorApp, txnId)
            if (coordinatorTxnStatus == TransactionStatus.ROLLED_BACK ||
                coordinatorTxnStatus == TransactionStatus.NO_TRANSACTION
            ) {
                // already rolled back by coordinator
                manager.dispose(txnId)
                return
            }
        }

        val rollbackObservation = micrometerContext.createObservation("ecos.txn.rollback")
            .highCardinalityKeyValue("txnId", txnId.toString())

        val catchStartTime = System.currentTimeMillis()
        rollbackObservation.observe {
            for (app in apps) {
                try {
                    remoteClient.rollback(app, txnId, error)
                } catch (rollbackErr: Throwable) {
                    rollbackObservation.observation.error(rollbackErr)
                    error.addSuppressed(rollbackErr)
                }
            }
        }

        actionsManager.executeActionsAfterRollback(txnId, txnLevel, error, actions)

        disposeRoot(txnId, apps, error)

        val rollbackTime = System.currentTimeMillis() - catchStartTime
        debug(txnId) { "Rollback executed in $rollbackTime ms" }
    }

    override fun disposeRoot(txnId: TxnId, apps: Collection<String>, mainError: Throwable?) {
        log.debug { "[$txnId] Dispose txn for apps $apps" }
        val appsToDispose = LinkedHashSet<String>(apps)
        appsToDispose.add(currentApp)
        for (app in appsToDispose) {
            try {
                remoteClient.disposeTxn(app, txnId)
            } catch (e: Throwable) {
                if (mainError != null) {
                    mainError.addSuppressed(e)
                } else {
                    log.error(e) { "[$txnId] Something went wrong while txn disposing for app $app" }
                }
            }
        }
    }

    private fun isRemoteCommitCoordinatorAvailable(): Boolean {
        val res = remoteClient.isApiVersionSupported(
            commitCoordinatorApp,
            TxnManagerRemoteApiClient.COORDINATE_COMMIT_VER
        )
        when (res) {
            ApiVersionRes.APP_NOT_AVAILABLE -> error("App $commitCoordinatorApp is not available")
            else -> {}
        }
        return res == ApiVersionRes.SUPPORTED
    }

    override fun runTxnRecovering(): Boolean {
        if (repo == NoopTwoPhaseCommitRepo) {
            return false
        }
        val result = AtomicBoolean()
        appLockApi.doInSyncOrSkip("ecos.txn.recovery") {
            result.set(recoverImpl())
        }
        return result.get()
    }

    private fun recoverImpl(): Boolean {

        log.trace { "Recovering started" }

        val data = repo.findDataToRecover() ?: return false

        log.info { "Found transactional data to recover: " + Json.mapper.toStringNotNull(data) }

        val appsToProc: Set<String>
        if (data.status == TwoPhaseCommitStatus.PREPARING) {
            appsToProc = data.data.apps.keys
            repo.beforeRollback(data.txnId, appsToProc)
        } else {
            appsToProc = data.appsToProcess
        }
        val isCommitting = data.status == TwoPhaseCommitStatus.COMMITTING

        val processedApps = HashSet<String>()
        val errors = HashMap<String, Throwable>()
        for (app in appsToProc) {
            try {
                val targetAppInstanceId = data.appRoutes[app]
                val webReqTargetApp = if (app == currentApp || targetAppInstanceId.isNullOrBlank()) {
                    app
                } else {
                    val instanceFullId = "$app:$targetAppInstanceId"
                    if (remoteClient.isAppAvailable(instanceFullId)) {
                        instanceFullId
                    } else {
                        app
                    }
                }
                val xids = data.data.apps[app] ?: emptySet()
                if (xids.isNotEmpty()) {
                    when (
                        remoteClient.isApiVersionSupported(
                            webReqTargetApp,
                            TxnManagerRemoteApiClient.COORDINATE_COMMIT_VER
                        )
                    ) {
                        ApiVersionRes.SUPPORTED -> {
                            if (isCommitting) {
                                remoteClient.recoveryCommit(webReqTargetApp, data.txnId, xids)
                            } else {
                                remoteClient.recoveryRollback(webReqTargetApp, data.txnId, xids)
                            }
                        }

                        ApiVersionRes.APP_NOT_AVAILABLE -> {
                            error("App '$app' is not available")
                        }

                        ApiVersionRes.NOT_SUPPORTED -> {
                            error("Recovery API is not supported by app '$app'")
                        }
                    }
                }
                processedApps.add(app)
            } catch (e: Throwable) {
                errors[app] = e
                val actionName = if (isCommitting) {
                    "committing"
                } else {
                    "rolling back"
                }
                log.error(e) { "Error while recovery $actionName of txn ${data.txnId} and app $app" }
            }
        }

        if (isCommitting) {
            repo.afterCommit(data.txnId, processedApps, errors)
        } else {
            repo.afterRollback(data.txnId, processedApps, errors)
        }

        if (errors.isEmpty()) {
            // if errors is empty, then everything processed successfully.
            // Let's try to execute transactional actions for alive apps
            // if target app is not alive, then action will be skipped
            val actionsType = if (isCommitting) {
                TxnActionType.AFTER_COMMIT
            } else {
                TxnActionType.AFTER_ROLLBACK
            }
            val actions = data.data.actions[actionsType] ?: emptyList()
            val appRefByName = HashMap<String, String>()
            val nonAliveApps = HashSet<String>()
            for (action in actions) {
                try {
                    val appRef = appRefByName.computeIfAbsent(action.appName) { name ->
                        val instanceId = data.appRoutes[action.appName] ?: ""
                        var appRef = ""
                        if (instanceId.isNotBlank()) {
                            appRef = "$name:$instanceId"
                        }
                        appRef
                    }
                    if (appRef.isNotBlank()) {
                        if (nonAliveApps.contains(appRef) || !remoteClient.isAppAvailable(appRef)) {
                            log.debug {
                                "Action $action can't be executed, because target app $appRef is not alive"
                            }
                            nonAliveApps.add(appRef)
                        } else {
                            actionsManager.executeActionById(data.txnId, actionsType, action.withApp(appRef))
                        }
                    }
                } catch (e: Throwable) {
                    log.error(e) { "Exception while $actionsType action execution. Action ref: $action" }
                }
            }
        }
        return true
    }

    private inline fun debug(txnId: TxnId, crossinline message: () -> String) {
        log.debug { "[$txnId] " + message() }
    }
}
