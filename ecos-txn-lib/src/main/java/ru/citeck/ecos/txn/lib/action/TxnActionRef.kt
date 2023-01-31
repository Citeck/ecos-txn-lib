package ru.citeck.ecos.txn.lib.action

data class TxnActionRef(
    val appName: String,
    val id: Int,
    val order: Float
)
