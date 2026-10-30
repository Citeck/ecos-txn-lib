package ru.citeck.ecos.txn.lib.resource.type.xa

import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.transaction.Status
import jakarta.transaction.Synchronization
import jakarta.transaction.Transaction
import jakarta.transaction.TransactionManager
import jakarta.transaction.TransactionSynchronizationRegistry
import ru.citeck.ecos.txn.lib.TxnContext
import ru.citeck.ecos.txn.lib.transaction.TransactionStatus
import ru.citeck.ecos.txn.lib.transaction.TransactionSynchronization
import ru.citeck.ecos.webapp.api.properties.EcosWebAppProps
import javax.transaction.xa.XAResource
import ru.citeck.ecos.txn.lib.transaction.Transaction as EcosTransaction

/**
 * JTA view of the current ECOS transaction for a managed pool of XA connections, as its transaction
 * manager and its synchronization registry. The pool registers an interposed synchronization for every
 * connection it lends; one batch per transaction keeps that from costing the root an action each.
 */
class JavaXaTxnManagerAdapter(val props: EcosWebAppProps) :
    TransactionManager,
    TransactionSynchronizationRegistry {

    companion object {
        private val log = KotlinLogging.logger {}

        private const val TXN_DATA_KEY = "java-xa-txn"

        private fun TransactionStatus.toJtaStatus(): Int {
            return when (this) {
                TransactionStatus.NEW -> Status.STATUS_ACTIVE
                TransactionStatus.ACTIVE -> Status.STATUS_ACTIVE
                TransactionStatus.PREPARING -> Status.STATUS_PREPARING
                TransactionStatus.PREPARED -> Status.STATUS_PREPARED
                TransactionStatus.COMMITTING -> Status.STATUS_COMMITTING
                TransactionStatus.COMMITTED -> Status.STATUS_COMMITTED
                TransactionStatus.ROLLING_BACK -> Status.STATUS_ROLLING_BACK
                TransactionStatus.ROLLED_BACK -> Status.STATUS_ROLLEDBACK
                TransactionStatus.DISPOSED -> Status.STATUS_NO_TRANSACTION
                TransactionStatus.NO_TRANSACTION -> Status.STATUS_NO_TRANSACTION
            }
        }
    }

    override fun begin() {
        error("not supported")
    }

    override fun commit() {
        error("not supported")
    }

    override fun getStatus(): Int {
        return transaction?.status ?: return Status.STATUS_NO_TRANSACTION
    }

    override fun getTransaction(): Transaction? {
        return currentAdapter()
    }

    override fun resume(tobj: Transaction?) {
        error("not supported")
    }

    override fun rollback() {
        error("not supported")
    }

    override fun setRollbackOnly() {
        error("not supported")
    }

    override fun setTransactionTimeout(seconds: Int) {}

    override fun suspend(): Transaction {
        error("not supported")
    }

    override fun getTransactionKey(): Any? {
        return TxnContext.getTxnOrNull()?.getId()
    }

    override fun getTransactionStatus(): Int {
        return status
    }

    override fun getRollbackOnly(): Boolean {
        requireActiveAdapter()
        return false
    }

    override fun registerInterposedSynchronization(sync: Synchronization) {
        requireActiveAdapter().registerInterposed(sync)
    }

    override fun putResource(key: Any, value: Any?) {
        requireActiveAdapter().resources[key] = value
    }

    override fun getResource(key: Any): Any? {
        return requireActiveAdapter().resources[key]
    }

    private fun currentAdapter(): TransactionAdapter? {
        return TxnContext.getTxnOrNull()?.let { ecosTxn ->
            ecosTxn.getData(TXN_DATA_KEY) { TransactionAdapter(ecosTxn) }
        }
    }

    /**
     * JTA refuses registry calls outside an active transaction. Joining a batch bypasses
     * [EcosTransaction.registerSync] and its own check, so the rule is applied here for every call.
     */
    private fun requireActiveAdapter(): TransactionAdapter {
        val adapter = currentAdapter() ?: error("No transaction is bound to the current thread")
        val status = adapter.transaction.getStatus()
        check(status == TransactionStatus.ACTIVE) {
            "[${adapter.transaction.getId()}] Transaction is not active: $status"
        }
        return adapter
    }

    private inner class TransactionAdapter(
        val transaction: EcosTransaction
    ) : Transaction {

        val resources = HashMap<Any, Any?>()

        private var interposedBatch: SynchronizationBatch? = null

        fun registerInterposed(sync: Synchronization) {
            if (interposedBatch?.tryAdd(sync) == true) {
                return
            }
            val batch = SynchronizationBatch(sync)
            transaction.registerSync(batch)
            interposedBatch = batch
        }

        override fun commit() {
            error("not supported")
        }

        override fun delistResource(xaRes: XAResource, flag: Int): Boolean {
            error("not supported")
        }

        override fun enlistResource(xaRes: XAResource): Boolean {
            transaction.getOrAddRes(IdentityKey(xaRes)) { _, txnId ->
                JavaXaTxnResourceAdapter(xaRes, "xa-res", txnId, props.appName, props.appInstanceId)
            }
            return true
        }

        override fun getStatus(): Int {
            return transaction.getStatus().toJtaStatus()
        }

        override fun registerSynchronization(sync: Synchronization) {
            transaction.registerSync(SynchronizationBatch(sync))
        }

        override fun rollback() {
            error("not supported")
        }

        override fun setRollbackOnly() {
            error("not supported")
        }
    }

    /**
     * JTA synchronizations behind one ECOS synchronization, in the order of registration. Members added
     * while [beforeCompletion] runs are called by it; afterwards the batch is closed, so a late member
     * gets a batch of its own and still runs before the commit.
     */
    private class SynchronizationBatch(first: Synchronization) : TransactionSynchronization {

        private val members = arrayListOf(first)
        private var closed = false

        /** @return false once the batch is closed: [sync] needs a batch of its own */
        fun tryAdd(sync: Synchronization): Boolean {
            if (closed) {
                return false
            }
            members.add(sync)
            return true
        }

        override fun beforeCompletion() {
            try {
                var idx = 0
                while (idx < members.size) {
                    members[idx++].beforeCompletion()
                }
            } finally {
                closed = true
            }
        }

        override fun afterCompletion(status: TransactionStatus) {
            val jtaStatus = when (status) {
                TransactionStatus.COMMITTED, TransactionStatus.ROLLED_BACK -> status.toJtaStatus()
                else -> return
            }
            for (member in members) {
                try {
                    member.afterCompletion(jtaStatus)
                } catch (e: Throwable) {
                    log.error(e) { "Exception in synchronization afterCompletion call" }
                }
            }
        }
    }

    private class IdentityKey(private val value: Any) {
        override fun equals(other: Any?): Boolean {
            return other is IdentityKey && value === other.value
        }
        override fun hashCode(): Int {
            return System.identityHashCode(value)
        }
        override fun toString(): String {
            return value.toString()
        }
    }
}
