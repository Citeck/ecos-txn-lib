package ru.citeck.ecos.txn.lib.resource.type.xa

import ru.citeck.ecos.txn.lib.transaction.TxnId
import java.util.*
import javax.transaction.xa.Xid

class TxnXid(id: TxnId, currentAppName: String, currentAppInstanceId: String) : Xid {

    companion object {
        private const val FORMAT_ID = 90
    }

    private val globalTxnIdBytes = id.toString().toByteArray(Charsets.UTF_8)

    private val txnBranchIdBytes = TxnId.create(
        currentAppName,
        currentAppInstanceId
    ).toString().toByteArray(Charsets.UTF_8)

    override fun getFormatId(): Int {
        return FORMAT_ID
    }

    override fun getGlobalTransactionId(): ByteArray {
        return globalTxnIdBytes
    }

    override fun getBranchQualifier(): ByteArray {
        return txnBranchIdBytes
    }

    override fun toString(): String {
        return formatId.toString() +
            "_" + Base64.getEncoder().encodeToString(globalTxnIdBytes) +
            "_" + Base64.getEncoder().encodeToString(txnBranchIdBytes)
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) {
            return true
        }
        if (javaClass != other?.javaClass) {
            return false
        }
        other as TxnXid
        if (!globalTxnIdBytes.contentEquals(other.globalTxnIdBytes) ||
            !txnBranchIdBytes.contentEquals(other.txnBranchIdBytes)
        ) {
            return false
        }
        return true
    }

    override fun hashCode(): Int {
        var result = globalTxnIdBytes.contentHashCode()
        result = 31 * result + txnBranchIdBytes.contentHashCode()
        return result
    }
}
