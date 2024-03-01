package ru.citeck.ecos.txn.lib.json

import ru.citeck.ecos.commons.json.JsonMapperConfigurer
import ru.citeck.ecos.commons.json.MappingContext
import ru.citeck.ecos.txn.lib.transaction.xid.TxnXidDeserializer
import ru.citeck.ecos.txn.lib.transaction.xid.TxnXidSerializer

class EcosTxnJsonMapperConfigurer : JsonMapperConfigurer {

    override fun configure(context: MappingContext) {
        configurePredicates(context)
    }

    private fun configurePredicates(context: MappingContext) {
        context.addDeserializer(TxnXidDeserializer())
        context.addSerializer(TxnXidSerializer())
    }
}
