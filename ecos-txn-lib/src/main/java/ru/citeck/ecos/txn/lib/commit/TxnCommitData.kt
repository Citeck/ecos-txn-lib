package ru.citeck.ecos.txn.lib.commit

import ru.citeck.ecos.txn.lib.action.TxnActionId
import ru.citeck.ecos.txn.lib.action.TxnActionType
import ru.citeck.ecos.txn.lib.transaction.xid.EcosXid

data class TxnCommitData(
    val apps: Map<String, Set<EcosXid>>,
    val actions: Map<TxnActionType, List<TxnActionId>>
) {
    companion object {
        val EMPTY = TxnCommitData(emptyMap(), emptyMap())
    }
}
