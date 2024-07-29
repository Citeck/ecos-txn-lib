package ru.citeck.ecos.txn.lib.resource.type.xa

import jakarta.transaction.Status
import jakarta.transaction.Synchronization
import jakarta.transaction.Transaction
import jakarta.transaction.TransactionManager
import ru.citeck.ecos.txn.lib.TxnContext
import ru.citeck.ecos.txn.lib.transaction.TransactionStatus
import ru.citeck.ecos.txn.lib.transaction.TransactionSynchronization
import ru.citeck.ecos.webapp.api.properties.EcosWebAppProps
import javax.transaction.xa.XAResource

class JavaXaTxnManagerAdapter(val props: EcosWebAppProps) : TransactionManager {

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
        return TxnContext.getTxnOrNull()?.let { ecosTxn ->
            ecosTxn.getData("java-xa-txn") { TransactionAdapter(ecosTxn) }
        }
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

    private inner class TransactionAdapter(
        val transaction: ru.citeck.ecos.txn.lib.transaction.Transaction
    ) : Transaction {

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
            return when (transaction.getStatus()) {
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

        override fun registerSynchronization(sync: Synchronization) {
            transaction.registerSync(object : TransactionSynchronization {
                override fun beforeCompletion() {
                    sync.beforeCompletion()
                }
                override fun afterCompletion(status: TransactionStatus) {
                    val statusId = when (status) {
                        TransactionStatus.COMMITTED -> Status.STATUS_COMMITTED
                        TransactionStatus.ROLLED_BACK -> Status.STATUS_ROLLEDBACK
                        else -> -1
                    }
                    if (statusId >= 0) {
                        sync.afterCompletion(statusId)
                    }
                }
            })
        }

        override fun rollback() {
            error("not supported")
        }

        override fun setRollbackOnly() {
            error("not supported")
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
