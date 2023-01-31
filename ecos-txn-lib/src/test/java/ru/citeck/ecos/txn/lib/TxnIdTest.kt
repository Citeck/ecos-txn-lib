package ru.citeck.ecos.txn.lib

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import ru.citeck.ecos.commons.json.Json
import ru.citeck.ecos.txn.lib.transaction.TxnId

class TxnIdTest {

    @Test
    fun test() {
        repeat(40 /* more than NUM_TO_STR_RADIX */) {
            testTxnId("emodel", "4k5hnzwxzmua", false)
        }
        testTxnId("emodel-abc", "!23eaqw-qed_Wc.z/.|?", true)
    }

    private fun testTxnId(appName: String, appInstanceId: String, print: Boolean) {

        val txnId = TxnId.create(appName, appInstanceId)
        if (print) {
            println(txnId)
        }

        val txnIdStr = Json.mapper.toStringNotNull(txnId)
        val txnIdFromStr = Json.mapper.readNotNull(txnIdStr, TxnId::class.java)

        assertThat(txnIdFromStr).isEqualTo(txnId)
    }

    @Test
    fun test2() {

        println('a' < 'z')

        for (i in 0..36) {
            println(i.toString(36) + " - " + i.toString(36)[0].code)
        }
    }
}
