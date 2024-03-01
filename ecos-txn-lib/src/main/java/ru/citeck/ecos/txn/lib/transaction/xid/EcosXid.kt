package ru.citeck.ecos.txn.lib.transaction.xid

import ru.citeck.ecos.txn.lib.transaction.TxnId
import java.lang.Exception
import java.util.*
import javax.transaction.xa.Xid

class EcosXid private constructor(
    private val formatId: Int,
    private val txnId: TxnId,
    private val branchQualifier: ByteArray
) : Xid {

    companion object {
        private const val FORMAT_ID = 90

        @JvmStatic
        fun create(txnId: TxnId, appName: String, appInstanceId: String): EcosXid {
            val branchId = TxnId.create(appName, appInstanceId).toString().toByteArray(Charsets.UTF_8)
            return create(FORMAT_ID, txnId, branchId)
        }

        @JvmStatic
        fun create(txnId: TxnId, branchQualifier: ByteArray): EcosXid {
            return create(FORMAT_ID, txnId, branchQualifier)
        }

        @JvmStatic
        fun create(xid: Xid): EcosXid {
            if (xid is EcosXid) {
                return xid
            }
            return create(xid.formatId, xid.globalTransactionId, xid.branchQualifier)
        }

        @JvmStatic
        fun create(formatId: Int, txnIdBytes: ByteArray, branchQualifier: ByteArray): EcosXid {
            val txnId = try {
                TxnId.valueOf(String(txnIdBytes, Charsets.UTF_8))
            } catch (e: Exception) {
                throw InvalidEcosXidException(formatId, txnIdBytes, branchQualifier, e)
            }
            return create(formatId, txnId, branchQualifier)
        }

        @JvmStatic
        fun create(formatId: Int, txnId: TxnId, branchQualifier: ByteArray): EcosXid {
            if (formatId != FORMAT_ID) {
                throw InvalidEcosXidException(formatId, txnId.toByteArray(), branchQualifier)
            }
            return EcosXid(formatId, txnId, branchQualifier)
        }

        private fun TxnId.toByteArray(): ByteArray {
            return this.toString().toByteArray(Charsets.UTF_8)
        }
    }

    private val globalTxnIdBytes = txnId.toString().toByteArray(Charsets.UTF_8)

    override fun getFormatId(): Int {
        return formatId
    }

    fun getTransactionId(): TxnId {
        return txnId
    }

    override fun getGlobalTransactionId(): ByteArray {
        return globalTxnIdBytes
    }

    override fun getBranchQualifier(): ByteArray {
        return branchQualifier
    }

    override fun toString(): String {
        return formatId.toString() +
            "_" + txnId +
            "_" + Base64.getEncoder().encodeToString(branchQualifier)
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) {
            return true
        }
        if (other !is Xid) {
            return false
        }
        return formatId == other.formatId &&
            globalTxnIdBytes.contentEquals(other.globalTransactionId) &&
            branchQualifier.contentEquals(other.branchQualifier)
    }

    override fun hashCode(): Int {
        var result = formatId
        result = 31 * result + globalTxnIdBytes.contentHashCode()
        result = 31 * result + branchQualifier.contentHashCode()
        return result
    }
}
