package ru.citeck.ecos.txn.lib

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.commons.json.Json
import ru.citeck.ecos.commons.json.JsonMapper
import ru.citeck.ecos.txn.lib.transaction.TxnId
import ru.citeck.ecos.txn.lib.transaction.xid.EcosXid

class EcosXidTest {

    @Test
    fun serializationTest() {

        val txnId = TxnId.create("emodel", "123")
        val xid = EcosXid.create(txnId, "aaa", "bbb")

        fun testWithMapper(mapper: JsonMapper, withToString: Boolean) {

            val xidJson = mapper.toJson(xid)
            assertThat(mapper.convert(xidJson, EcosXid::class.java)).isEqualTo(xid)

            if (withToString) {
                val xidString = mapper.toString(xid)
                assertThat(mapper.read(xidString, EcosXid::class.java)).isEqualTo(xid)
            }
            val xidBytes = mapper.toBytes(xid)
            assertThat(mapper.read(xidBytes, EcosXid::class.java)).isEqualTo(xid)

            val dtoBytes = mapper.toBytes(DtoTest(xid, xid, xid))
            assertThat(mapper.read(dtoBytes, DtoTest::class.java)).isEqualTo(DtoTest(xid, xid, xid))

            if (withToString) {
                val dtoString = mapper.toString(DtoTest(xid, xid, xid))
                assertThat(mapper.read(dtoString, DtoTest::class.java)).isEqualTo(DtoTest(xid, xid, xid))
            }
        }

        testWithMapper(Json.mapper, true)
        testWithMapper(Json.cborMapper, false)
    }

    data class DtoTest(
        val xid0: EcosXid,
        val xid1: EcosXid,
        val xid2: EcosXid
    )
}
