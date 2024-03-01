package ru.citeck.ecos.txn.lib.action

import ecos.com.fasterxml.jackson210.annotation.JsonIgnore

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
