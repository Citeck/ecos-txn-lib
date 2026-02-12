package ru.citeck.ecos.txn.lib

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import ru.citeck.ecos.txn.lib.transaction.TxnId
import ru.citeck.ecos.txn.lib.transaction.xid.EcosXid
import ru.citeck.ecos.txn.lib.transaction.xid.InvalidEcosXidException
import java.util.Base64
import javax.transaction.xa.Xid

class EcosXidExtendedTest {

    @Test
    fun `create from EcosXid returns same instance`() {
        val txnId = TxnId.create("app", "inst")
        val xid = EcosXid.create(txnId, "app", "inst")

        val result = EcosXid.create(xid)

        assertThat(result).isSameAs(xid)
    }

    @Test
    fun `create from foreign Xid copies data`() {
        val txnId = TxnId.create("app", "inst")
        val original = EcosXid.create(txnId, "app", "inst")

        val foreign = object : Xid {
            override fun getFormatId() = original.formatId
            override fun getGlobalTransactionId() = original.globalTransactionId
            override fun getBranchQualifier() = original.branchQualifier
        }

        val result = EcosXid.create(foreign)

        assertThat(result).isNotSameAs(foreign)
        assertThat(result.formatId).isEqualTo(original.formatId)
        assertThat(result.globalTransactionId).isEqualTo(original.globalTransactionId)
        assertThat(result.branchQualifier).isEqualTo(original.branchQualifier)
    }

    @Test
    fun `invalid format ID throws InvalidEcosXidException`() {
        val txnId = TxnId.create("app", "inst")

        assertThatThrownBy {
            EcosXid.create(99, txnId, "branch".toByteArray())
        }.isInstanceOf(InvalidEcosXidException::class.java)
    }

    @Test
    fun `invalid txnId bytes throws InvalidEcosXidException`() {
        assertThatThrownBy {
            EcosXid.create(90, "not-a-valid-txnid".toByteArray(), "branch".toByteArray())
        }.isInstanceOf(InvalidEcosXidException::class.java)
    }

    @Test
    fun `equals with foreign Xid implementation`() {
        val txnId = TxnId.create("app", "inst")
        val ecosXid = EcosXid.create(txnId, "app", "inst")

        val foreign = object : Xid {
            override fun getFormatId() = ecosXid.formatId
            override fun getGlobalTransactionId() = ecosXid.globalTransactionId.copyOf()
            override fun getBranchQualifier() = ecosXid.branchQualifier.copyOf()
        }

        assertThat(ecosXid).isEqualTo(foreign)
    }

    @Test
    fun `equals returns false for different format id`() {
        val txnId = TxnId.create("app", "inst")
        val ecosXid = EcosXid.create(txnId, "app", "inst")

        val foreign = object : Xid {
            override fun getFormatId() = 42
            override fun getGlobalTransactionId() = ecosXid.globalTransactionId.copyOf()
            override fun getBranchQualifier() = ecosXid.branchQualifier.copyOf()
        }

        assertThat(ecosXid).isNotEqualTo(foreign)
    }

    @Test
    fun `equals returns false for non-Xid`() {
        val txnId = TxnId.create("app", "inst")
        val ecosXid = EcosXid.create(txnId, "app", "inst")

        assertThat(ecosXid).isNotEqualTo("not-a-xid")
    }

    @Test
    fun `toString format`() {
        val txnId = TxnId.create("app", "inst")
        val xid = EcosXid.create(txnId, "app", "inst")

        val str = xid.toString()

        // Format: formatId_txnId_base64(branchQualifier)
        assertThat(str).startsWith("90_")
        assertThat(str).contains(txnId.toString())
        // branchQualifier is base64-encoded after second underscore
        val parts = str.split("_", limit = 3)
        assertThat(parts).hasSize(3)
        assertThat(parts[0]).isEqualTo("90")
        // Verify the base64 part can be decoded
        Base64.getDecoder().decode(parts[2])
    }

    @Test
    fun `getTransactionId returns correct TxnId`() {
        val txnId = TxnId.create("app", "inst")
        val xid = EcosXid.create(txnId, "app", "inst")

        assertThat(xid.getTransactionId()).isEqualTo(txnId)
    }

    @Test
    fun `hashCode is consistent for equal xids`() {
        val txnId = TxnId.create("app", "inst")
        val xid1 = EcosXid.create(txnId, "app", "inst")

        val xid2 = EcosXid.create(
            xid1.formatId,
            xid1.globalTransactionId,
            xid1.branchQualifier
        )

        assertThat(xid1.hashCode()).isEqualTo(xid2.hashCode())
        assertThat(xid1).isEqualTo(xid2)
    }

    @Test
    fun `create with txnId and branchQualifier bytes`() {
        val txnId = TxnId.create("app", "inst")
        val branch = "custom-branch".toByteArray()
        val xid = EcosXid.create(txnId, branch)

        assertThat(xid.getTransactionId()).isEqualTo(txnId)
        assertThat(xid.branchQualifier).isEqualTo(branch)
        assertThat(xid.formatId).isEqualTo(90)
    }
}
