package ru.citeck.ecos.txn.lib.transaction

/**
 * Independent transaction ancestry. An ordinary remote call preserves this value.
 */
data class TxnNesting(val parentId: TxnId, val depth: Int) {
    companion object {

        const val MAX_DEPTH = 64

        @JvmField
        val ROOT = TxnNesting(TxnId.EMPTY, 0)

        @JvmStatic
        fun of(transaction: Transaction?): TxnNesting {
            return (transaction as? TransactionImpl)?.nesting ?: ROOT
        }

        internal fun childOf(transaction: Transaction?): TxnNesting {
            // Completion callbacks may still own connections or run under a suspended ancestor.
            return if (transaction == null) {
                ROOT
            } else {
                TxnNesting(transaction.getId(), of(transaction).depth + 1)
            }
        }
    }

    init {
        require(depth in 0..MAX_DEPTH) { "Invalid transaction nesting depth: $depth" }
        require(parentId.isEmpty() == (depth == 0)) { "Transaction parent and depth disagree" }
    }
}
