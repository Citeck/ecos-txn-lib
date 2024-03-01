package ru.citeck.ecos.txn.lib.manager.recovery

import ru.citeck.ecos.txn.lib.transaction.xid.EcosXid

interface RecoverableStorage {

    fun getPreparedXids(): List<EcosXid>

    fun commitPrepared(xid: EcosXid)

    fun rollbackPrepared(xid: EcosXid)
}
