package ru.citeck.ecos.txn.lib.commit

import mu.KotlinLogging
import ru.citeck.ecos.commons.json.Json
import ru.citeck.ecos.txn.lib.action.TxnActionId
import ru.citeck.ecos.txn.lib.action.TxnActionType
import ru.citeck.ecos.txn.lib.commit.repo.NoopTwoPhaseCommitRepo
import ru.citeck.ecos.txn.lib.commit.repo.TwoPhaseCommitRepo
import ru.citeck.ecos.txn.lib.commit.repo.TwoPhaseCommitStatus
import ru.citeck.ecos.txn.lib.manager.TransactionManagerImpl
import ru.citeck.ecos.txn.lib.manager.api.TxnManagerWebExecutor
import ru.citeck.ecos.txn.lib.resource.CommitPrepareStatus
import ru.citeck.ecos.txn.lib.transaction.TxnId
import ru.citeck.ecos.webapp.api.web.client.EcosWebClientApi
import ru.citeck.ecos.webapp.api.web.client.EcosWebClientReq
import ru.citeck.ecos.webapp.api.web.client.EcosWebClientResp
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
    private val webClient = manager.webAppApi.getWebClientApi()
    private val commitCoordinatorApp = manager.props.commitCoordinatorApp
    private val micrometerContext = manager.micrometerContext
    private val remoteWebAppsApi = manager.webAppApi.getRemoteWebAppsApi()

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
                    if (appToCommit == currentApp) {
                        manager.getManagedTransaction(txnId).onePhaseCommit()
                    } else {
                        execRequest(appToCommit, txnId, TxnManagerWebExecutor.TYPE_ONE_PHASE_COMMIT, null) {}
                    }
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
                val commitReqBody = TxnManagerWebExecutor.TxnCommitReqBody(data, txnLevel)
                execRequest(
                    commitCoordinatorApp, txnId, TxnManagerWebExecutor.TYPE_COORDINATE_COMMIT,
                    {
                        it.body { w -> w.writeDto(commitReqBody) }
                    }
                ) {}
                return
            }
        }
        repo.beforePrepare(txnId, data)

        val preparedAppsToCommit = HashSet<String>()
        data.apps.forEach { (app, _) ->
            try {
                val status = if (app == currentApp) {
                    manager.getManagedTransaction(txnId).prepareCommit()
                } else {
                    execRequest(app, txnId, TxnManagerWebExecutor.TYPE_PREPARE_COMMIT, null) {
                        it.getBodyReader().readDto(TxnManagerWebExecutor.PrepareCommitResp::class.java).status
                    }
                }
                if (status == CommitPrepareStatus.PREPARED) {
                    preparedAppsToCommit.add(app)
                }
            } catch (mainError: Throwable) {
                repo.beforeRollback(txnId, data.apps.keys)
                rollbackRoot(
                    txnId,
                    preparedAppsToCommit,
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
                    if (appToCommit == currentApp) {
                        manager.getManagedTransaction(txnId).commitPrepared()
                    } else {
                        execRequest(appToCommit, txnId, TxnManagerWebExecutor.TYPE_COMMIT_PREPARED, null) {}
                    }
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
        disposeRoot(txnId, data.apps.keys, null)
    }

    override fun rollbackRoot(
        txnId: TxnId,
        apps: Collection<String>,
        actions: List<TxnActionId>?,
        error: Throwable,
        txnLevel: Int
    ) {

        val rollbackObservation = micrometerContext.createObservation("ecos.txn.rollback")
            .highCardinalityKeyValue("txnId", txnId.toString())

        val catchStartTime = System.currentTimeMillis()
        rollbackObservation.observe {
            for (app in apps) {
                try {
                    if (app == currentApp) {
                        manager.getManagedTransaction(txnId).rollback(error)
                    } else {
                        execRequest(app, txnId, TxnManagerWebExecutor.TYPE_ROLLBACK, null) {}
                    }
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
        for (app in apps) {
            try {
                if (app == currentApp) {
                    manager.dispose(txnId)
                } else {
                    execRequest(app, txnId, TxnManagerWebExecutor.TYPE_DISPOSE, null) {}
                }
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
        val apiVersion = webClient.getApiVersion(commitCoordinatorApp, TxnManagerWebExecutor.PATH, 1)
        when (apiVersion) {
            EcosWebClientApi.AV_APP_NOT_AVAILABLE -> error("App $commitCoordinatorApp is not available")
            EcosWebClientApi.AV_PATH_NOT_SUPPORTED -> error(
                "Path ${TxnManagerWebExecutor.PATH} doesn't supported " +
                    "in app $commitCoordinatorApp is not available"
            )
        }
        return apiVersion >= 1
    }

    override fun recover(): Boolean {
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
                    if (remoteWebAppsApi.isAppAvailable(instanceFullId)) {
                        instanceFullId
                    } else {
                        app
                    }
                }
                val xids = data.data.apps[app] ?: emptySet()
                if (xids.isNotEmpty()) {
                    if (app == currentApp) {
                        if (isCommitting) {
                            manager.getRecoveryManager().commitPrepared(xids.toList())
                        } else {
                            manager.getRecoveryManager().rollbackPrepared(xids.toList())
                        }
                    } else {
                        val apiVersion = webClient.getApiVersion(
                            webReqTargetApp,
                            TxnManagerWebExecutor.PATH,
                            1
                        )
                        when (apiVersion) {
                            EcosWebClientApi.AV_APP_NOT_AVAILABLE -> {
                                error("App '$app' is not available")
                            }

                            EcosWebClientApi.AV_PATH_NOT_SUPPORTED -> {
                                error("Path ${TxnManagerWebExecutor.PATH} is not supported by app '$app'")
                            }

                            EcosWebClientApi.AV_VERSION_NOT_SUPPORTED -> {
                                error("API version for path ${TxnManagerWebExecutor.PATH} is not supported by app '$app'")
                            }
                        }
                        if (apiVersion < 1) {
                            error("Application '$app' doesn't support recovery API")
                        }
                        val type = if (isCommitting) {
                            TxnManagerWebExecutor.TYPE_RECOVERY_COMMIT
                        } else {
                            TxnManagerWebExecutor.TYPE_RECOVERY_ROLLBACK
                        }
                        execRequest(
                            webReqTargetApp,
                            data.txnId,
                            type,
                            { it.header(TxnManagerWebExecutor.HEADER_XIDS, xids) }
                        ) {}
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
                        if (nonAliveApps.contains(appRef) || !remoteWebAppsApi.isAppAvailable(appRef)) {
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

    private fun <T> execRequest(
        appName: String,
        txnId: TxnId,
        type: String,
        custom: ((EcosWebClientReq) -> Unit)?,
        result: (EcosWebClientResp) -> T
    ): T {
        val req = webClient.newRequest()
            .targetApp(appName)
            .version(webClient.getApiVersion(appName, TxnManagerWebExecutor.PATH, TxnManagerWebExecutor.VERSION))
            .path(TxnManagerWebExecutor.PATH)
            .header(TxnManagerWebExecutor.HEADER_TYPE, type)
            .header(TxnManagerWebExecutor.HEADER_TXN_ID, txnId)

        custom?.invoke(req)

        return req.executeSync(result)
    }

    private inline fun debug(txnId: TxnId, crossinline message: () -> String) {
        log.debug { "[$txnId] " + message() }
    }
}
