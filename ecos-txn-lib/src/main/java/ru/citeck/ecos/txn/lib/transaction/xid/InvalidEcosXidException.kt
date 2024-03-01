package ru.citeck.ecos.txn.lib.transaction.xid

import java.lang.RuntimeException
import java.util.Base64

class InvalidEcosXidException : RuntimeException {

    companion object {
        private fun formatMsg(format: Int, globalId: ByteArray, branchId: ByteArray): String {
            return "Invalid xid: ${format}_" +
                Base64.getEncoder().encodeToString(globalId) + "_" +
                Base64.getEncoder().encodeToString(branchId)
        }
    }

    constructor(
        format: Int,
        globalId: ByteArray,
        branchId: ByteArray
    ) : super(formatMsg(format, globalId, branchId))

    constructor(
        format: Int,
        globalId: ByteArray,
        branchId: ByteArray,
        cause: Throwable
    ) : super(formatMsg(format, globalId, branchId), cause)
}
