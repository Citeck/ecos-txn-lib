package ru.citeck.ecos.txn.lib.manager.recovery

import ru.citeck.ecos.txn.lib.transaction.xid.EcosXid
import java.util.concurrent.CopyOnWriteArrayList

class RecoveryManagerImpl : RecoveryManager {

    private val storages = CopyOnWriteArrayList<RecoverableStorage>()

    override fun commitPrepared(xids: List<EcosXid>) {
        if (xids.isEmpty()) {
            return
        }
        val storageByXid = getPreparedXids()
        for (xid in xids) {
            storageByXid[xid]?.commitPrepared(xid)
        }
    }

    override fun rollbackPrepared(xids: List<EcosXid>) {
        if (xids.isEmpty()) {
            return
        }
        val storageByXid = getPreparedXids()
        for (xid in xids) {
            storageByXid[xid]?.rollbackPrepared(xid)
        }
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
