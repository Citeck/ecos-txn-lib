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

    fun drainActions(type: TxnActionType): List<TxnActionId> {
        val actionsRes = actions.remove(type) ?: return emptyList()
        return actionsRes.map { it.ref.getGlobalId() }
    }

    fun getActions(type: TxnActionType): List<TxnActionId> {
        return actions[type]?.map { it.ref.getGlobalId() } ?: emptyList()
    }

    fun addAction(type: TxnActionType, actionRef: TxnActionRef) {
        actions.computeIfAbsent(type) { TreeSet() }.add(
            TxnActionRefWithIdx(actionRef, index.getAndIncrement())
        )
    }

    fun executeBeforeCommitActions() {
        actions[TxnActionType.BEFORE_COMMIT]?.forEach {
            actionsManager.executeActionById(txnId, TxnActionType.BEFORE_COMMIT, it.ref.getGlobalId())
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
