package ru.citeck.ecos.txn.lib.manager

import io.github.oshai.kotlinlogging.KotlinLogging
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.txn.lib.transaction.TransactionStatus
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference
import kotlin.concurrent.thread

class TxnManagerJob(private val manager: TransactionManagerImpl) {

    companion object {
        private const val HEALTH_CHECK_PERIOD_MS = 20_000
        private const val MAX_NON_ALIVE_TIME_MS = 60_000
        private const val RECOVERY_PERIOD_MS = 30_000

        private val log = KotlinLogging.logger {}
    }

    private val jobThreadActive = AtomicBoolean(true)
    private val remoteClient = manager.remoteClient
    private val currentAppName = manager.webAppApi.getProperties().appName

    private var shutdownHookThread: Thread = thread(start = false) { jobThreadActive.set(false) }

    @Synchronized
    fun start() {

        // already stopped
        if (!jobThreadActive.get()) {
            return
        }

        val nextRecoveringTime = AtomicReference(
            System.currentTimeMillis() + RECOVERY_PERIOD_MS
        )

        thread(start = true, name = "ecos-txn-manager-job") {
            while (jobThreadActive.get()) {
                Thread.sleep(10000)
                try {
                    AuthContext.runAsSystem {
                        disposeInactiveTransactions()
                        if (System.currentTimeMillis() > nextRecoveringTime.get()) {
                            manager.commitCoordinator.runTxnRecovering()
                            nextRecoveringTime.set(System.currentTimeMillis() + RECOVERY_PERIOD_MS)
                        }
                    }
                } catch (e: Throwable) {
                    if (e is InterruptedException) {
                        Thread.currentThread().interrupt()
                        throw e
                    }
                    log.error(e) { "Exception in ecos-txn-manager-job" }
                }
            }
        }
        Runtime.getRuntime().addShutdownHook(shutdownHookThread)
    }

    private fun disposeInactiveTransactions() {
        val transactionsById = manager.transactionsById
        if (transactionsById.isEmpty()) {
            return
        }
        val prevHealthCheckTime = System.currentTimeMillis() - HEALTH_CHECK_PERIOD_MS
        val nonAliveTime = System.currentTimeMillis() - MAX_NON_ALIVE_TIME_MS
        for (txnId in transactionsById.keys) {
            val txnInfo = transactionsById[txnId] ?: continue
            if (
                txnInfo.managerCanRecoverPreparedTxn &&
                txnInfo.transaction.getStatus() == TransactionStatus.PREPARED
            ) {
                // prepared transaction will wait until transaction manager commit or rollback it
                continue
            }
            if (!txnInfo.transaction.isIdle()) {
                txnInfo.lastAliveTime = System.currentTimeMillis()
                continue
            }
            val lastActiveTime = txnInfo.transaction.getLastActiveTime().toEpochMilli()
            if (txnInfo.lastAliveTime < lastActiveTime) {
                txnInfo.lastAliveTime = lastActiveTime
            }
            if (txnInfo.lastAliveTime < prevHealthCheckTime &&
                (txnId.appName != currentAppName || txnInfo.commitDelegatedToApp.isNotEmpty())
            ) {
                val txnManagerApp = txnInfo.commitDelegatedToApp.ifEmpty {
                    txnId.appName + ":" + txnId.appInstanceId
                }
                if (remoteClient.isAppAvailable(txnManagerApp)) {
                    try {
                        val status = remoteClient.getTxnStatus(txnManagerApp, txnId)
                        if (status == TransactionStatus.NO_TRANSACTION) {
                            log.warn {
                                "[$txnId] Remote app $txnManagerApp returned 'NO_TRANSACTION' " +
                                    "but transaction is still exists in this instance. " +
                                    "Local status: ${txnInfo.transaction.getStatus()}"
                            }
                            txnInfo.lastAliveTime = 0L
                        } else {
                            txnInfo.lastAliveTime = System.currentTimeMillis()
                        }
                    } catch (e: Throwable) {
                        log.warn(e) { "[$txnId] Exception while transaction status checking" }
                    }
                }
            }
            if (txnInfo.lastAliveTime < nonAliveTime) {
                log.warn { "[$txnId] Dispose stuck transaction" }
                try {
                    manager.dispose(txnId)
                } catch (e: Throwable) {
                    log.error(e) { "[$txnId] Error while disposing of stuck transaction" }
                }
            }
        }
    }

    @Synchronized
    fun stop() {
        jobThreadActive.set(false)
        Runtime.getRuntime().removeShutdownHook(shutdownHookThread)
    }
}
