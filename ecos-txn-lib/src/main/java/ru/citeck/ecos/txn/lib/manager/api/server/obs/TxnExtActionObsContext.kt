package ru.citeck.ecos.txn.lib.manager.api.server.obs

import ru.citeck.ecos.commons.data.DataValue
import ru.citeck.ecos.micrometer.obs.EcosObsContext

class TxnExtActionObsContext(
    val type: String,
    val data: DataValue,
    val apiVer: Int
) : EcosObsContext(NAME) {

    companion object {
        const val NAME = "ecos.txn.external-action"
    }
}
