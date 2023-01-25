package ru.citeck.ecos.txn.lib

import java.time.Instant

data class EcosTxnId(
    val appName: String,
    val appInstanceId: String,
    val created: Instant,
    val index: Long
) {


}
