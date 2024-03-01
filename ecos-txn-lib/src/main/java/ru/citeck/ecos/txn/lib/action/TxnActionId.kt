package ru.citeck.ecos.txn.lib.action

class TxnActionId(
    val appName: String,
    val localId: Int
) {

    fun withApp(newApp: String): TxnActionId {
        return TxnActionId(newApp, localId)
    }

    override fun toString(): String {
        return "$appName/$localId"
    }
}
