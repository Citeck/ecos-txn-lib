package ru.citeck.ecos.txn.lib.manager.recovery

import ru.citeck.ecos.txn.lib.transaction.xid.EcosXid

interface RecoveryManager {

    fun commitPrepared(xids: List<EcosXid>)

    fun rollbackPrepared(xids: List<EcosXid>)

    fun registerStorage(storage: RecoverableStorage)
}
