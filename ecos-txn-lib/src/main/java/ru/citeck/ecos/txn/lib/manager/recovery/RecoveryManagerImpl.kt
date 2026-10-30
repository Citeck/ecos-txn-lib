package ru.citeck.ecos.txn.lib.manager.recovery

import ru.citeck.ecos.txn.lib.commit.repo.JournalPresence
import ru.citeck.ecos.txn.lib.transaction.TxnId
import ru.citeck.ecos.txn.lib.transaction.xid.EcosXid
import java.time.Instant
import java.util.concurrent.CopyOnWriteArrayList

class RecoveryManagerImpl : RecoveryManager {

    private val storages = CopyOnWriteArrayList<RecoverableStorage>()

    /**
     * Keep known outcomes to reject contradictory decisions within this process.
     * After restart, a complete storage scan makes an absent branch an idempotent retry.
     */
    private val completed = object : LinkedHashMap<EcosXid, Boolean>() {
        override fun removeEldestEntry(eldest: MutableMap.MutableEntry<EcosXid, Boolean>): Boolean {
            return size > 10000
        }
    }

    /**
     * Preserve a completed live transaction's outcome before releasing its context.
     * The marker also proves completion of a participant without XA resources.
     */
    @Synchronized
    internal fun rememberCompleted(txnId: TxnId, xids: Collection<EcosXid>, commit: Boolean) {
        val receipts = xids + EcosXid.create(txnId, ByteArray(0))
        check(receipts.all { completed[it] == null || completed[it] == commit }) {
            "Conflicting completion outcome for transaction $txnId"
        }
        receipts.forEach { completed[it] = commit }
    }

    override fun commitPrepared(xids: Collection<EcosXid>) {
        completePrepared(xids, true)
    }

    override fun rollbackPrepared(xids: Collection<EcosXid>) {
        completePrepared(xids, false)
    }

    @Synchronized
    private fun completePrepared(xids: Collection<EcosXid>, commit: Boolean) {
        if (xids.isEmpty()) {
            return
        }
        check(storages.isNotEmpty() || xids.all { it.branchQualifier.isEmpty() || completed[it] == commit }) {
            "No recoverable storages are registered and completion is not proven"
        }

        val storageByXid = getPreparedXids()
        val txnIds = xids.mapTo(HashSet()) { it.getTransactionId() }
        for (txnId in txnIds) {
            val outcome = completed[EcosXid.create(txnId, ByteArray(0))]
            check(outcome == null || outcome == commit) {
                "Conflicting recovery decision for transaction $txnId"
            }
        }
        for (xid in xids) {
            check(completed[xid] == null || completed[xid] == commit) {
                "Recovery outcome is unknown for Xid $xid; retain coordinator data for reconciliation"
            }
        }

        /**
         * Older participants could enlist during beforeCompletion after the journal
         * snapshot. Include all visible branches of the requested global transactions,
         * covering branches omitted by a lost or partial prepare response.
         */
        for ((xid, storage) in storageByXid) {
            if (xid.getTransactionId() !in txnIds) {
                continue
            }
            check(completed[xid] == null || completed[xid] == commit) {
                "Conflicting recovery decision for Xid $xid"
            }
            if (commit) {
                storage.commitPrepared(xid)
            } else {
                storage.rollbackPrepared(xid)
            }
            completed[xid] = commit
        }
        /**
         * Every source must have been registered before recovery and enumerated
         * successfully above. A branch already completed before a lost reply is
         * absent after restart; repeating the same decision must acknowledge it.
         * Scan failures and known conflicting outcomes still propagate.
         */
        xids.forEach { completed[it] = commit }
        txnIds.forEach { completed[EcosXid.create(it, ByteArray(0))] = commit }
    }

    /**
     * Enumerate every registered source before making any decision. Journal RPCs
     * must not hold the monitor used by ordinary completion and recovery.
     */
    internal fun rollbackOldOrphans(
        cutoff: Instant,
        journalPresence: (TxnId) -> JournalPresence,
        isTransactionActive: (TxnId) -> Boolean
    ): Boolean {

        val txnIds = synchronized(this) {
            getPreparedBranches().mapTo(LinkedHashSet()) { it.first.getTransactionId() }
        }
        var complete = true
        for (txnId in txnIds) {
            if (txnId.isEmpty() ||
                txnId.appName.isBlank() ||
                txnId.appInstanceId.isBlank() ||
                txnId.created <= Instant.EPOCH ||
                !txnId.created.isBefore(cutoff)
            ) {
                continue
            }
            if (synchronized(this) { hasCommittedOutcome(txnId) || isTransactionActive(txnId) }) {
                continue
            }
            when (journalPresence(txnId)) {
                JournalPresence.PRESENT -> continue
                JournalPresence.UNKNOWN -> {
                    complete = false
                    continue
                }
                JournalPresence.ABSENT -> rollbackOrphan(txnId, isTransactionActive)
            }
        }
        return complete
    }

    /**
     * Ordinary recovery may have completed branches while the journal was being
     * queried. Refresh branches and recheck local proof before any rollback.
     */
    @Synchronized
    private fun rollbackOrphan(txnId: TxnId, isTransactionActive: (TxnId) -> Boolean) {

        if (hasCommittedOutcome(txnId) || isTransactionActive(txnId)) {
            return
        }
        val prepared = getPreparedBranches().filter { it.first.getTransactionId() == txnId }
        if (hasCommittedOutcome(txnId) || isTransactionActive(txnId)) {
            return
        }
        for ((xid, storage) in prepared) {
            storage.rollbackPrepared(xid)
            completed[xid] = false
            completed[EcosXid.create(txnId, ByteArray(0))] = false
        }
    }

    private fun hasCommittedOutcome(txnId: TxnId): Boolean {
        return completed.any { (xid, commit) -> commit && xid.getTransactionId() == txnId }
    }

    private fun getPreparedBranches(): List<Pair<EcosXid, RecoverableStorage>> {
        return storages.flatMap { storage -> storage.getPreparedXids().map { it to storage } }
    }

    private fun getPreparedXids(): Map<EcosXid, RecoverableStorage> {
        val result = HashMap<EcosXid, RecoverableStorage>()
        storages.forEach { storage ->
            storage.getPreparedXids().forEach { xid ->
                result[xid] = storage
            }
        }
        return result
    }

    override fun registerStorage(storage: RecoverableStorage) {
        storages.add(storage)
    }
}
