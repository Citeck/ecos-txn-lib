package ru.citeck.ecos.txn.lib

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import ru.citeck.ecos.txn.lib.transaction.TxnId
import java.time.Instant

class TxnIdExtendedTest {

    @Test
    fun `valueOf with null returns EMPTY`() {
        assertThat(TxnId.valueOf(null)).isSameAs(TxnId.EMPTY)
    }

    @Test
    fun `valueOf with blank returns EMPTY`() {
        assertThat(TxnId.valueOf("")).isSameAs(TxnId.EMPTY)
        assertThat(TxnId.valueOf("   ")).isSameAs(TxnId.EMPTY)
    }

    @Test
    fun `valueOf with wrong part count throws`() {
        assertThatThrownBy {
            TxnId.valueOf("a:b:c")
        }.hasMessageContaining("Invalid txn id")
    }

    @Test
    fun `valueOf with unsupported version throws`() {
        assertThatThrownBy {
            TxnId.valueOf("1:abc:app:inst:0")
        }.hasMessageContaining("Unsupported txnId version")
    }

    @Test
    fun `EMPTY isEmpty and isNotEmpty`() {
        assertThat(TxnId.EMPTY.isEmpty()).isTrue()
        assertThat(TxnId.EMPTY.isNotEmpty()).isFalse()
    }

    @Test
    fun `created TxnId isNotEmpty`() {
        val txnId = TxnId.create("app", "inst")
        assertThat(txnId.isEmpty()).isFalse()
        assertThat(txnId.isNotEmpty()).isTrue()
    }

    @Test
    fun `unique index increments`() {
        val id1 = TxnId.create("app", "inst")
        val id2 = TxnId.create("app", "inst")

        assertThat(id2.index).isGreaterThan(id1.index)
    }

    @Test
    fun `toString of EMPTY returns empty string`() {
        assertThat(TxnId.EMPTY.toString()).isEqualTo("")
    }

    @Test
    fun `toString roundtrip via valueOf`() {
        val original = TxnId.create("my-app", "my-instance")
        val serialized = original.toString()
        val deserialized = TxnId.valueOf(serialized)

        assertThat(deserialized).isEqualTo(original)
        assertThat(deserialized.appName).isEqualTo("my-app")
        assertThat(deserialized.appInstanceId).isEqualTo("my-instance")
        assertThat(deserialized.version).isEqualTo(0)
    }

    @Test
    fun `equals and hashCode symmetry`() {
        val id1 = TxnId.create("app", "inst")
        val id2 = TxnId.valueOf(id1.toString())

        assertThat(id1).isEqualTo(id2)
        assertThat(id2).isEqualTo(id1)
        assertThat(id1.hashCode()).isEqualTo(id2.hashCode())
    }

    @Test
    fun `not equal to different txn id`() {
        val id1 = TxnId.create("app-a", "inst")
        val id2 = TxnId.create("app-b", "inst")

        assertThat(id1).isNotEqualTo(id2)
    }

    @Test
    fun `not equal to null or other type`() {
        val id = TxnId.create("app", "inst")

        assertThat(id).isNotEqualTo(null)
        assertThat(id.equals("string")).isFalse()
    }

    @Test
    fun `create with explicit Instant preserves timestamp`() {
        val timestamp = Instant.ofEpochMilli(1700000000000L)
        val txnId = TxnId.create("app", "inst", timestamp)

        assertThat(txnId.created.toEpochMilli()).isEqualTo(1700000000000L)
        assertThat(txnId.appName).isEqualTo("app")
        assertThat(txnId.appInstanceId).isEqualTo("inst")
    }

    @Test
    fun `toString is cached`() {
        val txnId = TxnId.create("app", "inst")

        val str1 = txnId.toString()
        val str2 = txnId.toString()

        // Should return the same string object (cached)
        assertThat(str1).isSameAs(str2)
    }
}
