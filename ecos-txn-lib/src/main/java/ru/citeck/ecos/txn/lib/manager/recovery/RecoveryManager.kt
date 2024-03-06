package ru.citeck.ecos.txn.lib.manager.recovery

import ru.citeck.ecos.txn.lib.transaction.xid.EcosXid

interface RecoveryManager {

    fun commitPrepared(xids: Collection<EcosXid>)

    fun rollbackPrepared(xids: Collection<EcosXid>)

    fun registerStorage(storage: RecoverableStorage)
}
