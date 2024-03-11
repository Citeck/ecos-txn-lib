package ru.citeck.ecos.txn.lib.commit

import mu.KotlinLogging
import ru.citeck.ecos.commons.json.Json
import ru.citeck.ecos.txn.lib.action.TxnActionId
import ru.citeck.ecos.txn.lib.action.TxnActionType
import ru.citeck.ecos.txn.lib.commit.obs.TxnCommitObsContext
import ru.citeck.ecos.txn.lib.commit.obs.TxnRollbackObsContext
import ru.citeck.ecos.txn.lib.commit.repo.NoopTwoPhaseCommitRepo
import ru.citeck.ecos.txn.lib.commit.repo.TwoPhaseCommitRepo
import ru.citeck.ecos.txn.lib.commit.repo.TwoPhaseCommitStatus
import ru.citeck.ecos.txn.lib.manager.TransactionManagerImpl
import ru.citeck.ecos.txn.lib.manager.api.client.ApiVersionRes
import ru.citeck.ecos.txn.lib.manager.api.client.TxnManagerRemoteApiClient
import ru.citeck.ecos.txn.lib.transaction.TransactionStatus
import ru.citeck.ecos.txn.lib.transaction.TxnId
import java.util.concurrent.CopyOnWriteArrayList

class CommitCoordinatorImpl(
    private val repo: TwoPhaseCommitRepo,
    private val manager: TransactionManagerImpl
) : CommitCoordinator {

    companion object {
        private val log = KotlinLogging.logger {}
    }

    private val appLockApi = manager.webAppApi.getAppLockApi()
    private val currentAppName: String = manager.webAppApi.getProperties().appName
    private val currentAppInstanceId: String = manager.webAppApi.getProperties().appInstanceId
    private val currentAppRef = "$currentAppName:$currentAppInstanceId"
    private val actionsManager = manager.actionsManager
    private val remoteClient = manager.remoteClient
    private val commitCoordinatorApp = manager.props.commitCoordinatorApp
    private val micrometerContext = manager.micrometerContext

    private val localTransactionsToRecover = CopyOnWriteArrayList<TxnId>()

    override fun commitRoot(txnId: TxnId, data: TxnCommitData, txnLevel: Int) {

        if (data.apps.isEmpty() && data.actions.isEmpty()) {
            manager.dispose(txnId)
            return
        }

        val commitObservation = micrometerContext.createObs(
            TxnCommitObsContext(txnId, data, txnLevel, manager)
        )

        if (data.apps.size == 1) {
            val appEntry = data.apps.entries.first()
            if (appEntry.value.size == 1) {
                val appToCommit = appEntry.key
                commitObservation.observe {
                    remoteClient.onePhaseCommit(appToCommit, txnId)
                }
                actionsManager.executeActionsAfterCommit(txnId, txnLevel, data.actions[TxnActionType.AFTER_COMMIT]).finally {
                    disposeRoot(txnId, setOf(appToCommit), null)
                }
                return
            }
        }

        val txnApps = HashSet(data.apps.keys)
        data.actions.forEach { (_, v) -> v.forEach { txnApps.add(it.appName) } }

        if (commitCoordinatorApp.isNotBlank() && commitCoordinatorApp != currentAppName) {
            if (!isRemoteCommitCoordinatorAvailable()) {
                log.debug {
                    "Commit coordinator app $commitCoordinatorApp doesn't support required web-api path."
                }
            } else {
                remoteClient.coordinateCommit(commitCoordinatorApp, txnId, data, txnLevel)
                if (!txnApps.contains(currentAppName)) {
                    manager.dispose(txnId)
                } else {
                    manager.transactionsById[txnId]?.commitDelegatedToApp = commitCoordinatorApp
                }
                return
            }
        }
        repo.beforePrepare(txnId, data)

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

        // after this line transaction become eventually committed
        // if something went wrong after this line, then we should not
        // roll back transaction. In that case we should just wait until
        // transaction will be committed by recovery mechanism
        repo.beforeCommit(txnId, preparedAppsToCommit)

        var commitError: Throwable? = null
        val commitErrors = HashMap<String, Throwable>()
        val committedApps = HashSet<String>()
        if (preparedAppsToCommit.isEmpty()) {
            repo.afterCommit(txnId, emptySet(), emptyMap())
        } else {
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
        }
        if (commitError == null) {
            actionsManager.executeActionsAfterCommit(txnId, txnLevel, data.actions[TxnActionType.AFTER_COMMIT])
                .finally {
                    disposeRoot(txnId, txnApps, null)
                }
        } else {
            if (repo != NoopTwoPhaseCommitRepo) {
                localTransactionsToRecover.add(txnId)
                disposeRoot(txnId, committedApps, commitError, disposeCurrentApp = false)
            } else {
                disposeRoot(txnId, txnApps, null)
            }
            throw commitError
        }
    }

    override fun rollbackRoot(
        txnId: TxnId,
        apps: Collection<String>,
        actions: List<TxnActionId>?,
        error: Throwable,
        txnLevel: Int
    ) {

        val commitDelegator = manager.transactionsById[txnId]?.commitDelegatedToApp ?: ""
        if (commitDelegator.isNotBlank()) {
            try {
                val coordinatorTxnStatus = remoteClient.getTxnStatus(commitDelegator, txnId)
                if (coordinatorTxnStatus == TransactionStatus.ROLLED_BACK ||
                    coordinatorTxnStatus == TransactionStatus.NO_TRANSACTION
                ) {
                    // already rolled back by coordinator
                    manager.dispose(txnId)
                    return
                }
            } catch (e: Throwable) {
                error.addSuppressed(e)
            }
        }

        val rollbackObservation = micrometerContext.createObs(
            TxnRollbackObsContext(
                txnId,
                apps,
                actions ?: emptyList(),
                error,
                txnLevel,
                manager
            )
        )

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

    override fun disposeRoot(
        txnId: TxnId,
        apps: Collection<String>,
        mainError: Throwable?,
        disposeCurrentApp: Boolean
    ) {
        log.debug { "[$txnId] Dispose txn for apps $apps" }

        val appsToDispose = if (disposeCurrentApp) {
            val appsToDispose = LinkedHashSet<String>(apps)
            appsToDispose.add(currentAppName)
            appsToDispose
        } else {
            apps
        }
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

    override fun runTxnRecovering() {
        if (repo == NoopTwoPhaseCommitRepo) {
            return
        }
        if (localTransactionsToRecover.isNotEmpty()) {
            val recoveredLocalTransactions = HashSet<TxnId>()
            for (txnId in localTransactionsToRecover) {
                if (recoverForData(repo.getRecoveryData(txnId))) {
                    recoveredLocalTransactions.add(txnId)
                }
            }
            localTransactionsToRecover.removeAll(recoveredLocalTransactions)
        }
        appLockApi.doInSyncOrSkip("ecos.txn.recovery") {
            var data = repo.findDataToRecover()
            var iterations = 10
            while (data != null && --iterations > 0) {
                recoverForData(data)
                data = repo.findDataToRecover()
            }
        }
    }

    private fun recoverForData(recoveryData: RecoveryData?): Boolean {

        recoveryData ?: return true

        log.info { "Begin transaction recovering: " + Json.mapper.toStringNotNull(recoveryData) }

        val appsToProc: Set<String>
        if (recoveryData.status == TwoPhaseCommitStatus.PREPARING) {
            appsToProc = recoveryData.data.apps.keys
            repo.beforeRollback(recoveryData.txnId, appsToProc)
        } else {
            appsToProc = recoveryData.appsToProcess
        }
        val isCommitting = recoveryData.status == TwoPhaseCommitStatus.COMMITTING

        val processedApps = HashSet<String>()
        val errors = HashMap<String, Throwable>()
        val aliveAppsInstances = HashSet<String>()
        for (app in appsToProc) {
            var webReqTargetApp = app
            try {
                val targetAppInstanceId = recoveryData.appRoutes[app]
                if (app == currentAppName) {
                    if (targetAppInstanceId == currentAppInstanceId) {
                        aliveAppsInstances.add(app)
                    }
                } else if (!targetAppInstanceId.isNullOrBlank()) {
                    val instanceFullId = "$app:$targetAppInstanceId"
                    if (remoteClient.isAppAvailable(instanceFullId)) {
                        aliveAppsInstances.add(instanceFullId)
                        webReqTargetApp = instanceFullId
                    }
                }
                val xids = recoveryData.data.apps[app] ?: emptySet()
                if (xids.isNotEmpty()) {
                    when (
                        remoteClient.isApiVersionSupported(
                            webReqTargetApp,
                            TxnManagerRemoteApiClient.COORDINATE_COMMIT_VER
                        )
                    ) {
                        ApiVersionRes.SUPPORTED -> {
                            if (isCommitting) {
                                remoteClient.recoveryCommit(webReqTargetApp, recoveryData.txnId, xids)
                            } else {
                                remoteClient.recoveryRollback(webReqTargetApp, recoveryData.txnId, xids)
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
                aliveAppsInstances.remove(webReqTargetApp)
                errors[app] = e
                val actionName = if (isCommitting) {
                    "committing"
                } else {
                    "rolling back"
                }
                log.error(e) { "Error while recovery $actionName of txn ${recoveryData.txnId} and app $app" }
            }
        }

        if (isCommitting) {
            repo.afterCommit(recoveryData.txnId, processedApps, errors)
        } else {
            repo.afterRollback(recoveryData.txnId, processedApps, errors)
        }

        if (errors.isNotEmpty()) {
            return false
        }
        // if errors is empty, then everything processed successfully.
        // Let's try to execute transactional actions for alive apps
        // if target app is not alive, then action will be skipped
        val actionsType = if (isCommitting) {
            TxnActionType.AFTER_COMMIT
        } else {
            TxnActionType.AFTER_ROLLBACK
        }
        val actions = recoveryData.data.actions[actionsType] ?: emptyList()
        val appRefByName = HashMap<String, String>()
        val nonAliveApps = HashSet<String>()
        for (action in actions) {
            try {
                val appRef = appRefByName.computeIfAbsent(action.appName) { name ->
                    val instanceId = recoveryData.appRoutes[action.appName] ?: ""
                    var appRef = ""
                    if (instanceId.isNotBlank()) {
                        appRef = "$name:$instanceId"
                    }
                    appRef
                }
                if (appRef.isNotBlank()) {
                    if (currentAppRef != appRef &&
                        (nonAliveApps.contains(appRef) || !remoteClient.isAppAvailable(appRef))
                    ) {
                        log.debug {
                            "Action $action can't be executed, because target app $appRef is not alive"
                        }
                        nonAliveApps.add(appRef)
                    } else {
                        val actionToExec = if (currentAppRef == appRef) {
                            aliveAppsInstances.add(action.appName)
                            action
                        } else {
                            aliveAppsInstances.add(appRef)
                            action.withApp(appRef)
                        }
                        actionsManager.executeActionById(recoveryData.txnId, actionsType, actionToExec)
                    }
                }
            } catch (e: Throwable) {
                log.error(e) { "Exception while $actionsType action execution. Action ref: $action" }
            }
        }
        disposeRoot(recoveryData.txnId, aliveAppsInstances, null, false)
        return true
    }

    private inline fun debug(txnId: TxnId, crossinline message: () -> String) {
        log.debug { "[$txnId] " + message() }
    }
}
