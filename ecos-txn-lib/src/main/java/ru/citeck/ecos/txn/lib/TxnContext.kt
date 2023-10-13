package ru.citeck.ecos.txn.lib

import ru.citeck.ecos.txn.lib.action.TxnActionType
import ru.citeck.ecos.txn.lib.manager.TransactionManager
import ru.citeck.ecos.txn.lib.manager.TransactionPolicy
import ru.citeck.ecos.txn.lib.transaction.Transaction
import ru.citeck.ecos.webapp.api.func.UncheckedConsumer
import ru.citeck.ecos.webapp.api.func.UncheckedRunnable
import ru.citeck.ecos.webapp.api.func.UncheckedSupplier
import ru.citeck.ecos.webapp.api.func.asKtFunc

object TxnContext {

    private lateinit var manager: TransactionManager
    private var managerExists = false

    fun <T> doInTxn(action: () -> T): T {
        return doInTxn(readOnly = false, action)
    }

    fun <T> doInTxn(readOnly: Boolean, action: () -> T): T {
        return doInTxn(readOnly, requiresNew = false, action)
    }

    @JvmStatic
    @JvmOverloads
    fun doInTxnJ(readOnly: Boolean = false, action: UncheckedRunnable) {
        doInTxn(readOnly, action.asKtFunc())
    }

    @JvmStatic
    @JvmOverloads
    fun <T> doInTxnJ(readOnly: Boolean = false, action: UncheckedSupplier<T>): T {
        return doInTxn(readOnly, action.asKtFunc())
    }

    fun <T> doInNewTxn(action: () -> T): T {
        return doInNewTxn(readOnly = false, action)
    }

    fun <T> doInNewTxn(readOnly: Boolean, action: () -> T): T {
        return doInTxn(readOnly, requiresNew = true, action)
    }

    @JvmStatic
    @JvmOverloads
    fun doInNewTxnJ(readOnly: Boolean = false, action: UncheckedRunnable) {
        doInNewTxn(readOnly, action.asKtFunc())
    }

    @JvmStatic
    @JvmOverloads
    fun <T> doInNewTxnJ(readOnly: Boolean = false, action: UncheckedSupplier<T>): T {
        return doInNewTxn(readOnly, action.asKtFunc())
    }

    @JvmStatic
    fun <T> doInTxn(readOnly: Boolean, requiresNew: Boolean, action: () -> T): T {
        if (!managerExists) {
            return action.invoke()
        }
        val policy = if (requiresNew) {
            TransactionPolicy.REQUIRES_NEW
        } else {
            TransactionPolicy.REQUIRED
        }
        return manager.doInTxn(policy, readOnly) { action.invoke() }
    }

    fun doBeforeCommit(order: Float, action: () -> Unit) {
        val txn = getTxnOrNull() ?: return action.invoke()
        txn.addAction(TxnActionType.BEFORE_COMMIT, order, false, action)
    }

    @JvmStatic
    fun doBeforeCommitJ(order: Float, action: UncheckedRunnable) {
        doBeforeCommit(order, action.asKtFunc())
    }

    fun doAfterCommit(order: Float, async: Boolean, action: () -> Unit) {
        val txn = getTxnOrNull() ?: return action.invoke()
        txn.addAction(TxnActionType.AFTER_COMMIT, order, async, action)
    }

    @JvmStatic
    fun doAfterCommitJ(order: Float, async: Boolean, action: UncheckedRunnable) {
        doAfterCommit(order, async, action.asKtFunc())
    }

    fun doAfterRollback(order: Float, async: Boolean, action: () -> Unit) {
        val txn = getTxnOrNull() ?: return
        txn.addAction(TxnActionType.AFTER_ROLLBACK, order, async, action)
    }

    @JvmStatic
    fun doAfterRollbackJ(order: Float, async: Boolean, action: UncheckedRunnable) {
        doAfterRollback(order, async, action.asKtFunc())
    }

    @JvmStatic
    fun <T> processSetAfterCommitJ(key: Any, element: T, action: UncheckedConsumer<Set<T>>) {
        processSetAfterCommit(key, element, action.asKtFunc())
    }

    fun <T> processSetAfterCommit(key: Any, element: T, action: (Set<T>) -> Unit) {
        processSet(key, TxnActionType.AFTER_COMMIT, element, action)
    }

    @JvmStatic
    fun <T> processListAfterCommitJ(key: Any, element: T, action: UncheckedConsumer<List<T>>) {
        processListAfterCommit(key, element, action.asKtFunc())
    }

    fun <T> processListAfterCommit(key: Any, element: T, action: (List<T>) -> Unit) {
        processList(key, TxnActionType.AFTER_COMMIT, element, action)
    }

    @JvmStatic
    fun <T> processSetBeforeCommitJ(key: Any, element: T, action: UncheckedConsumer<Set<T>>) {
        processSetBeforeCommit(key, element, action.asKtFunc())
    }

    fun <T> processSetBeforeCommit(key: Any, element: T, action: (Set<T>) -> Unit) {
        processSet(key, TxnActionType.BEFORE_COMMIT, element, action)
    }

    @JvmStatic
    fun <T> processListBeforeCommitJ(key: Any, element: T, action: UncheckedConsumer<List<T>>) {
        processListBeforeCommit(key, element, action.asKtFunc())
    }

    fun <T> processListBeforeCommit(key: Any, element: T, action: (List<T>) -> Unit) {
        processList(key, TxnActionType.BEFORE_COMMIT, element, action)
    }

    private fun <T> processSet(key: Any, actionType: TxnActionType, element: T, action: (Set<T>) -> Unit) {
        processCollection(key, actionType, element, 0f, false, { LinkedHashSet() }, action)
    }

    private fun <T> processList(key: Any, actionType: TxnActionType, element: T, action: (List<T>) -> Unit) {
        processCollection(key, actionType, element, 0f, false, { ArrayList() }, action)
    }

    private fun <T, C : MutableCollection<T>> processCollection(
        key: Any,
        actionType: TxnActionType,
        element: T,
        order: Float,
        async: Boolean,
        createNewCollection: () -> C,
        action: (C) -> Unit
    ) {

        val txn = getTxnOrNull()
        if (txn == null) {
            val collection = createNewCollection()
            collection.add(element)
            action.invoke(collection)
            return
        }
        val collection = txn.getData(key) { createNewCollection() }
        if (collection.isEmpty()) {
            txn.addAction(actionType, order, async) { action.invoke(collection) }
        }
        collection.add(element)
    }

    @JvmStatic
    fun getTxn(): Transaction {
        return getTxnOrNull() ?: error("Transaction doesn't exists")
    }

    @JvmStatic
    fun getTxnOrNull(): Transaction? {
        if (!managerExists) {
            return null
        }
        return manager.getCurrentTransaction()
    }

    @JvmStatic
    fun isReadOnly(): Boolean {
        return getTxnOrNull()?.isReadOnly() == true
    }

    fun setManager(manager: TransactionManager) {
        this.manager = manager
        managerExists = true
    }

    fun getManager(): TransactionManager {
        return this.manager
    }
}
