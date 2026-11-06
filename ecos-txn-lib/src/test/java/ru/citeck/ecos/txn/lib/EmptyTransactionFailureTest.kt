package ru.citeck.ecos.txn.lib

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import ru.citeck.ecos.test.commons.EcosWebAppApiMock
import ru.citeck.ecos.txn.lib.commit.repo.TwoPhaseCommitRepo
import ru.citeck.ecos.txn.lib.manager.TransactionManagerImpl
import ru.citeck.ecos.txn.lib.manager.TransactionPolicy
import java.lang.reflect.Proxy

class EmptyTransactionFailureTest {

    @Test
    fun `failure before resource enlistment does not invoke the distributed journal`() {
        val app = EcosWebAppApiMock("empty-failure")
        val calls = mutableListOf<String>()
        val journal = Proxy.newProxyInstance(
            TwoPhaseCommitRepo::class.java.classLoader,
            arrayOf(TwoPhaseCommitRepo::class.java)
        ) { _, method, _ ->
            calls.add(method.name)
            null
        } as TwoPhaseCommitRepo

        val manager = TransactionManagerImpl()
        manager.init(app, twoPhaseCommitRepo = journal)
        val failure = IllegalStateException("pool acquisition failed")

        try {
            assertThatThrownBy {
                manager.doInTxn(TransactionPolicy.REQUIRES_NEW, false) {
                    throw failure
                }
            }.isSameAs(failure)

            assertThat(calls).isEmpty()
            assertThat(manager.getCurrentTransaction()).isNull()
        } finally {
            manager.shutdown()
            app.dispose()
        }
    }
}
