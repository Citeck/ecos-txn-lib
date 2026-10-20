package ru.citeck.ecos.txn.lib.manager.api.server.action

import ru.citeck.ecos.txn.lib.commit.repo.JournalPresence
import ru.citeck.ecos.txn.lib.manager.api.client.TxnManagerRemoteApiClient
import ru.citeck.ecos.txn.lib.transaction.TxnId

class TmJournalPresenceAction : AbstractTmRemoteAction<TmJournalPresenceAction.Data>() {

    companion object {
        const val TYPE = "journal-presence"
    }

    override fun execute(data: Data, apiVer: Int): Resp {
        check(apiVer >= TxnManagerRemoteApiClient.JOURNAL_PRESENCE_VER) { "Journal lookup requires API v${TxnManagerRemoteApiClient.JOURNAL_PRESENCE_VER}" }
        return Resp(manager.getJournalPresence(data.coordinatorApp, data.txnId))
    }

    override fun getType(): String {
        return TYPE
    }

    data class Data(val coordinatorApp: String, val txnId: TxnId)

    data class Resp(val presence: JournalPresence = JournalPresence.UNKNOWN)
}
