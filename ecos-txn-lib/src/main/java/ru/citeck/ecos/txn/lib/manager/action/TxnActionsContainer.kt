package ru.citeck.ecos.txn.lib.manager.action

import ru.citeck.ecos.txn.lib.action.TxnActionId
import ru.citeck.ecos.txn.lib.action.TxnActionRef
import ru.citeck.ecos.txn.lib.action.TxnActionType
import ru.citeck.ecos.txn.lib.manager.TransactionManagerImpl
import ru.citeck.ecos.txn.lib.transaction.TxnId
import java.util.*
import java.util.concurrent.atomic.AtomicInteger

class TxnActionsContainer(
    val txnId: TxnId,
    val manager: TransactionManagerImpl
) {

    private val index = AtomicInteger()
    private val actions: MutableMap<TxnActionType, MutableSet<TxnActionRefWithIdx>> = EnumMap(TxnActionType::class.java)
    private val actionsManager = manager.actionsManager
    private var pendingBeforeCommit: TreeSet<TxnActionRefWithIdx>? = null

    fun getActions(type: TxnActionType): List<TxnActionId> {
        return actions[type]?.map { it.ref.getGlobalId() } ?: emptyList()
    }

    fun addAction(type: TxnActionType, actionRef: TxnActionRef) {
        val action = TxnActionRefWithIdx(actionRef, index.getAndIncrement())
        actions.computeIfAbsent(type) { TreeSet() }.add(action)
        if (type == TxnActionType.BEFORE_COMMIT) {
            pendingBeforeCommit?.add(action)
        }
    }

    fun executeBeforeCommitActions() {
        val actions = actions[TxnActionType.BEFORE_COMMIT] ?: return
        val executed = HashSet<TxnActionId>()
        val pending = TreeSet(actions)
        pendingBeforeCommit = pending
        var discoveryRounds = 0
        var knownActionCount = actions.size
        try {
            while (true) {
                val next = pending.pollFirst() ?: return
                val globalId = next.ref.getGlobalId()
                if (globalId in executed) {
                    continue
                }
                if (actions.size > knownActionCount) {
                    check(++discoveryRounds <= 1000) { "[$txnId] Before-commit actions did not converge" }
                    knownActionCount = actions.size
                }
                // Pick again after every execution so new ordinary actions precede
                // queued synchronization flushes without rescanning completed work.
                actionsManager.executeActions(txnId, TxnActionType.BEFORE_COMMIT, listOf(globalId)) { actionId ->
                    actionsManager.executeActionById(txnId, TxnActionType.BEFORE_COMMIT, actionId)
                    executed.add(actionId)
                }
            }
        } finally {
            pendingBeforeCommit = null
        }
    }

    private class TxnActionRefWithIdx(val ref: TxnActionRef, val index: Int) : Comparable<TxnActionRefWithIdx> {

        override fun compareTo(other: TxnActionRefWithIdx): Int {
            val res = ref.order.compareTo(other.ref.order)
            if (res != 0) {
                return res
            }
            return index.compareTo(other.index)
        }
    }
}
