package ru.citeck.ecos.txn.lib.manager

import ru.citeck.ecos.txn.lib.transaction.TxnId

/**
 * Transactions whose local branch was rolled back before it voted. The web layer recreates an empty
 * context for a request to a missing branch, and a late prepare in that context would vote
 * "nothing to commit", letting the coordinator commit the other branches.
 * Only failures land here, so the bounds are generous; a restart forgets them.
 */
internal class AbortedBranches(
    private val ttlMs: Long = 60 * 60_000L,
    private val maxSize: Int = 100_000,
    private val clock: () -> Long = System::currentTimeMillis
) {

    private val abortedAt = LinkedHashMap<TxnId, Long>()

    @Synchronized
    fun add(txnId: TxnId) {
        abortedAt.remove(txnId)
        abortedAt[txnId] = clock()
        evict()
    }

    @Synchronized
    fun contains(txnId: TxnId): Boolean {
        evict()
        return abortedAt.containsKey(txnId)
    }

    private fun evict() {
        val expiredBefore = clock() - ttlMs
        val entries = abortedAt.entries.iterator()
        while (entries.hasNext()) {
            val entry = entries.next()
            if (abortedAt.size <= maxSize && entry.value >= expiredBefore) {
                return
            }
            entries.remove()
        }
    }
}
