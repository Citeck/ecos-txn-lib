package ru.citeck.ecos.txn.lib.action

import com.fasterxml.jackson.annotation.JsonIgnore

data class TxnActionRef(
    val appName: String,
    val id: Int,
    val order: Float
) {

    private val actionId = TxnActionId(appName, id)

    @JsonIgnore
    fun getGlobalId(): TxnActionId {
        return actionId
    }
}
