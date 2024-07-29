package ru.citeck.ecos.txn.lib.transaction.xid

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.JsonToken
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.SerializerProvider
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import com.fasterxml.jackson.databind.ser.std.StdSerializer
import ru.citeck.ecos.txn.lib.transaction.TxnId

class TxnXidSerializer : StdSerializer<EcosXid>(EcosXid::class.java) {

    override fun serialize(value: EcosXid, gen: JsonGenerator, provider: SerializerProvider) {
        gen.writeStartArray()
        gen.writeNumber(value.formatId)
        gen.writeString(value.getTransactionId().toString())
        gen.writeBinary(value.branchQualifier)
        gen.writeEndArray()
    }
}

class TxnXidDeserializer : StdDeserializer<EcosXid>(EcosXid::class.java) {

    override fun deserialize(p: JsonParser, ctxt: DeserializationContext): EcosXid {
        return when (p.currentToken) {
            JsonToken.START_ARRAY -> {
                val formatIdToken = p.nextToken()
                if (formatIdToken != JsonToken.VALUE_NUMBER_INT) {
                    return ctxt.handleUnexpectedToken(EcosXid::class.java, p) as EcosXid
                }
                val formatId = p.numberValue.toInt()
                val txnIdToken = p.nextToken()
                if (txnIdToken != JsonToken.VALUE_STRING) {
                    return ctxt.handleUnexpectedToken(EcosXid::class.java, p) as EcosXid
                }
                val txnId = TxnId.valueOf(p.text)
                p.nextToken()
                val branchIdBytes = p.binaryValue
                p.nextToken() // end array
                return EcosXid.create(formatId, txnId, branchIdBytes)
            }
            else -> {
                ctxt.handleUnexpectedToken(EcosXid::class.java, p) as EcosXid
            }
        }
    }
}
