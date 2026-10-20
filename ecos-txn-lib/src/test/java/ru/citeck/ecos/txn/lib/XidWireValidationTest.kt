package ru.citeck.ecos.txn.lib

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.DynamicTest
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestFactory
import ru.citeck.ecos.commons.json.Json
import ru.citeck.ecos.txn.lib.transaction.TxnId
import ru.citeck.ecos.txn.lib.transaction.xid.EcosXid

class XidWireValidationTest {

    @TestFactory
    fun `malformed Xid tuples cannot become recovery identifiers`(): List<DynamicTest> {
        val id = TxnId.create("wire", "instance").toString()
        val malformed = linkedMapOf(
            "object" to "{}",
            "scalar" to "42",
            "string format" to "[\"90\",\"$id\",\"\"]",
            "missing txn" to "[90]",
            "numeric txn" to "[90,1,\"\"]",
            "invalid txn" to "[90,\"bad-id\",\"\"]",
            "missing branch" to "[90,\"$id\"]",
            "invalid base64" to "[90,\"$id\",\"!invalid!\"]",
            "unknown format" to "[91,\"$id\",\"\"]",
            "overflowed format" to "[4294967386,\"$id\",\"\"]",
            "extra element" to "[90,\"$id\",\"\",123]"
        )
        return malformed.map { (name, json) ->
            DynamicTest.dynamicTest(name) {
                assertThatThrownBy { Json.mapper.readNotNull(json, EcosXid::class.java) }
                    .describedAs("Malformed tuple: %s", name).isInstanceOf(Exception::class.java)
            }
        }
    }

    @Test
    fun `binary branches and empty global markers retain tuple boundaries in JSON and CBOR`() {
        val id = TxnId.create("wire", "instance")
        val values = listOf(
            EcosXid.create(id, byteArrayOf(0, -1, 127, -128)),
            EcosXid.create(id, ByteArray(0))
        )
        for (mapper in listOf(Json.mapper, Json.cborMapper)) {
            val envelope = Envelope(values, "after-xids")
            val decoded = mapper.readNotNull(mapper.toBytes(envelope)!!, Envelope::class.java)
            assertThat(decoded).isEqualTo(envelope)
            assertThat(decoded.xids.map { it.branchQualifier.toList() })
                .containsExactly(listOf<Byte>(0, -1, 127, -128), emptyList())
        }
    }

    data class Envelope(val xids: List<EcosXid>, val tail: String)
}
