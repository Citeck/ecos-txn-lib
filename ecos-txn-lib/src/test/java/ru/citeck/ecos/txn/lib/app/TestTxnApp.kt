package ru.citeck.ecos.txn.lib.app

import ru.citeck.ecos.micrometer.EcosMicrometerContext
import ru.citeck.ecos.test.commons.EcosWebAppApiMock
import ru.citeck.ecos.txn.lib.commit.repo.NoopTwoPhaseCommitRepo
import ru.citeck.ecos.txn.lib.commit.repo.TwoPhaseCommitRepo
import ru.citeck.ecos.txn.lib.manager.EcosTxnProps
import ru.citeck.ecos.txn.lib.manager.TransactionManagerImpl
import ru.citeck.ecos.txn.lib.manager.api.server.TxnManagerRemoteActions
import ru.citeck.ecos.txn.lib.manager.api.server.TxnManagerWebExecutor

class TestTxnApp(
    val name: String,
    val props: EcosTxnProps
) {

    val webApi = EcosWebAppApiMock(name)
    val txnManager = TransactionManagerImpl()
    val txnProps: EcosTxnProps = EcosTxnProps()
    val remoteActions = TxnManagerRemoteActions(txnManager)
    val executor = TxnManagerWebExecutor(remoteActions)

    fun init(commitRepo: TwoPhaseCommitRepo = NoopTwoPhaseCommitRepo) {
        txnManager.init(webApi, props, EcosMicrometerContext.NOOP, commitRepo)

        webApi.webClientExecuteImpl = { app, path, data ->
        }
    }
}
