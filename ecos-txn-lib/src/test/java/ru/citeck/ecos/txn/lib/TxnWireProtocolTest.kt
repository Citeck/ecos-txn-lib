package ru.citeck.ecos.txn.lib

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.DynamicTest
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestFactory
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.txn.lib.action.TxnActionId
import ru.citeck.ecos.txn.lib.action.TxnActionType
import ru.citeck.ecos.txn.lib.commit.TxnCommitData
import ru.citeck.ecos.txn.lib.commit.repo.JournalPresence
import ru.citeck.ecos.txn.lib.manager.api.client.ApiVersionRes
import ru.citeck.ecos.txn.lib.manager.api.client.TxnManagerRemoteApiClient
import ru.citeck.ecos.txn.lib.support.TxnWireHarness
import ru.citeck.ecos.txn.lib.transaction.TransactionStatus
import ru.citeck.ecos.txn.lib.transaction.TxnId
import ru.citeck.ecos.txn.lib.transaction.xid.EcosXid
import ru.citeck.ecos.webapp.api.web.client.EcosWebClientApi

class TxnWireProtocolTest {

    private val id = TxnId.create("origin-app", "previous-instance")
    private val xid = EcosXid.create(id, byteArrayOf(0, -1, 17))

    @TestFactory
    fun `existing transaction operations preserve routing ids and semantics across wire versions`(): List<DynamicTest> {

        val operations = listOf("get-status", "one-phase-commit", "commit-prepared", "rollback", "dispose", "exec-action")
        return (0..3).flatMap { version ->
            operations.map { operation ->
                DynamicTest.dynamicTest("v$version $operation") {
                    val wire = TxnWireHarness(version)
                    when (operation) {
                        "get-status" -> assertThat(wire.client.getTxnStatus("participant:instance", id))
                            .isEqualTo(TransactionStatus.PREPARED)
                        "one-phase-commit" -> wire.client.onePhaseCommit("participant:instance", id)
                        "commit-prepared" -> wire.client.commitPrepared("participant:instance", id)
                        "rollback" -> wire.client.rollback("participant:instance", id, null)
                        "dispose" -> wire.client.disposeTxn("participant:instance", id)
                        "exec-action" -> wire.client.executeTxnAction("participant:instance", id, 37)
                    }
                    val request = wire.requests.single()
                    assertThat(request.target).isEqualTo("participant:instance")
                    assertThat(request.requestPath).isEqualTo("/txn")
                    assertThat(request.wireVersion).isEqualTo(version)
                    val managerCall = wire.calls.first()
                    assertThat(managerCall.args.first()).isEqualTo(id)
                    val expected = when (operation) {
                        "get-status" -> "getStatus"
                        "dispose" -> "dispose"
                        else -> "getManagedTransaction"
                    }
                    assertThat(managerCall.method).isEqualTo(expected)
                    if (operation !in listOf("get-status", "dispose")) {
                        val txnCall = wire.calls.last()
                        assertThat(txnCall.method).isEqualTo(
                            "txn." + when (operation) {
                                "one-phase-commit" -> "onePhaseCommit"
                                "commit-prepared" -> "commitPrepared"
                                "exec-action" -> "executeAction"
                                else -> "rollback"
                            }
                        )
                        if (operation == "exec-action") assertThat(txnCall.args).containsExactly(37)
                        if (operation == "rollback") assertThat(txnCall.args).containsExactly(null)
                    }
                }
            }
        }
    }

    @TestFactory
    fun `prepare response and recovery ownership match negotiated protocol`(): List<DynamicTest> {

        return (0..3).flatMap { version ->
            listOf(false, true).map { hasResources ->
                DynamicTest.dynamicTest("prepare v$version resources=$hasResources") {
                    val wire = TxnWireHarness(version)
                    wire.prepared = if (hasResources) listOf(xid) else emptyList()
                    val result = wire.client.prepareCommit("participant", id)
                    assertThat(wire.calls.single().method).isEqualTo("prepareCommitFromExtManager")
                    assertThat(wire.calls.single().args).containsExactly(id, version > 0)
                    if (!hasResources) {
                        assertThat(result).isEmpty()
                    } else if (version <= 1) {
                        assertThat(result).containsExactly(EcosXid.create(id, byteArrayOf()))
                    } else {
                        assertThat(result).containsExactly(xid)
                    }
                }
            }
        }
    }

    @TestFactory
    fun `modern recovery and coordination preserve full payloads`(): List<DynamicTest> {

        return (2..3).flatMap { version ->
            listOf("recovery-commit", "recovery-rollback", "coordinate-commit").map { type ->
                DynamicTest.dynamicTest("v$version $type payload") {
                    val wire = TxnWireHarness(version)
                    val data = TxnCommitData(
                        mapOf("participant" to setOf(xid)),
                        mapOf(TxnActionType.AFTER_COMMIT to listOf(TxnActionId("participant", 9)))
                    )
                    when (type) {
                        "recovery-commit" -> wire.client.recoveryCommit("coordinator", id, setOf(xid))
                        "recovery-rollback" -> wire.client.recoveryRollback("coordinator", id, setOf(xid))
                        else -> wire.client.coordinateCommit("coordinator", id, data, 4)
                    }
                    val call = wire.calls.single()
                    assertThat(call.args[0]).isEqualTo(id)
                    if (type == "coordinate-commit") {
                        assertThat(call.method).isEqualTo("coordinateCommit")
                        val received = call.args[1] as TxnCommitData
                        assertThat(received.apps).isEqualTo(data.apps)
                        val action = received.actions[TxnActionType.AFTER_COMMIT]!!.single()
                        assertThat(action.appName).isEqualTo("participant")
                        assertThat(action.localId).isEqualTo(9)
                        assertThat(call.args[2]).isEqualTo(4)
                    } else {
                        assertThat(call.method).isEqualTo(if (type == "recovery-commit") "recoveryCommit" else "recoveryRollback")
                        assertThat(call.args[1] as Collection<*>).containsExactly(xid)
                    }
                }
            }
        }
    }

    @TestFactory
    fun `legacy executor accepts its supported coordinate payload without client delegation`(): List<DynamicTest> {

        return (0..1).map { version ->
            DynamicTest.dynamicTest("legacy coordinate v$version") {
                val wire = TxnWireHarness(version)
                val data = TxnCommitData(mapOf("participant" to setOf(xid)), emptyMap())
                val request = wire.Request().also {
                    it.wireVersion = version
                    it.header("type", "coordinate-commit")
                    it.header("txnId", id)
                    it.requestBody.writeDto(mapOf("data" to data, "txnLevel" to 7))
                }
                AuthContext.runAsSystem { request.dispatch() }
                val call = wire.calls.single()
                assertThat(call.method).isEqualTo("coordinateCommit")
                assertThat(call.args).containsExactly(id, data, 7)
            }
        }
    }

    @TestFactory
    fun `unsupported legacy recovery and delegation never reach manager`(): List<DynamicTest> {

        return (0..1).flatMap { version ->
            listOf("commit", "rollback", "coordinate").map { action ->
                DynamicTest.dynamicTest("legacy v$version rejects $action") {
                    val wire = TxnWireHarness(version)
                    assertThatThrownBy {
                        when (action) {
                            "commit" -> wire.client.recoveryCommit("peer", id, setOf(xid))
                            "rollback" -> wire.client.recoveryRollback("peer", id, setOf(xid))
                            else -> wire.client.coordinateCommit("peer", id, TxnCommitData.EMPTY, 0)
                        }
                    }.hasMessageContaining("Unsupported action type")
                    assertThat(wire.calls).isEmpty()
                    assertThat(wire.requests).isEmpty()
                }
            }
        }
    }

    @TestFactory
    fun `rollback transport errors propagate or attach to original cause`(): List<DynamicTest> {

        return (0..3).flatMap { version ->
            listOf(false, true).map { hasCause ->
                DynamicTest.dynamicTest("rollback v$version hasCause=$hasCause") {
                    val wire = TxnWireHarness(version)
                    val failure = IllegalStateException("participant unavailable")
                    wire.transportFailure = failure
                    if (hasCause) {
                        val original = IllegalArgumentException("business error")
                        wire.client.rollback("peer", id, original)
                        assertThat(original.suppressed).containsExactly(failure)
                    } else {
                        assertThatThrownBy { wire.client.rollback("peer", id, null) }.isSameAs(failure)
                    }
                    assertThat(wire.calls).isEmpty()
                }
            }
        }
    }

    @TestFactory
    fun `capability failures never dispatch an operation`(): List<DynamicTest> {

        return listOf(
            EcosWebClientApi.AV_APP_NOT_AVAILABLE,
            EcosWebClientApi.AV_PATH_NOT_SUPPORTED,
            EcosWebClientApi.AV_VERSION_NOT_SUPPORTED
        ).map { code ->
            DynamicTest.dynamicTest("capability code=$code") {
                val wire = TxnWireHarness(code)
                assertThat(wire.client.isApiVersionSupported("peer", 3)).isEqualTo(
                    if (code == EcosWebClientApi.AV_VERSION_NOT_SUPPORTED) ApiVersionRes.NOT_SUPPORTED else ApiVersionRes.APP_NOT_AVAILABLE
                )
                assertThat(wire.client.getJournalPresence("peer", id)).isEqualTo(JournalPresence.UNKNOWN)
                assertThatThrownBy { wire.client.getTxnStatus("peer", id) }.isInstanceOf(IllegalStateException::class.java)
                assertThat(wire.calls).isEmpty()
                assertThat(wire.requests).isEmpty()
            }
        }
    }

    @Test
    fun `single new protocol revision provides global recovery and journal lookup while v2 stays unsupported`() {

        assertThat(TxnManagerRemoteApiClient.CURRENT_API_VERSION).isEqualTo(3)
        assertThat(TxnManagerRemoteApiClient.GLOBAL_RECOVERY_VER).isEqualTo(3)
        assertThat(TxnManagerRemoteApiClient.JOURNAL_PRESENCE_VER).isEqualTo(3)
        val old = TxnWireHarness(2)
        assertThat(old.client.getJournalPresence("coordinator", id)).isEqualTo(JournalPresence.UNKNOWN)
        assertThat(old.requests).isEmpty()
        val current = TxnWireHarness(3)
        assertThat(current.executor.getApiVersion()).isEqualTo(0 to 3)
        assertThat(current.executor.getPath()).isEqualTo("/txn")
        assertThat(current.executor.isReadOnly()).isFalse()
        current.presence = JournalPresence.ABSENT
        assertThat(current.client.getJournalPresence("coordinator", id)).isEqualTo(JournalPresence.ABSENT)
        assertThat(current.calls.single().args).containsExactly("coordinator", id)
        assertThat(current.requests.single().wireVersion).isEqualTo(3)
    }

    @Test
    fun `availability follows the registry for the exact target instance`() {

        val wire = TxnWireHarness(3)
        assertThat(wire.client.isAppAvailable("participant:up")).isTrue()
        wire.available = false
        assertThat(wire.client.isAppAvailable("participant:down")).isFalse()
        assertThat(wire.availabilityRequests).containsExactly("participant:up", "participant:down")
    }

    @TestFactory
    fun `executor rejects non-system requests before any manager access`(): List<DynamicTest> {

        return (0..3).map { version ->
            DynamicTest.dynamicTest("unauthorized v$version") {
                val wire = TxnWireHarness(version)
                val request = wire.Request().also {
                    it.wireVersion = version
                    it.header("type", "rollback")
                    it.header("txnId", id)
                    it.requestBody.writeDto(mapOf("txnId" to id))
                }
                AuthContext.runAs("ordinary-user") {
                    assertThatThrownBy { request.dispatch() }.hasMessageContaining("Permission denied")
                }
                assertThat(wire.calls).isEmpty()
            }
        }
    }

    @TestFactory
    fun `executor rejects malformed headers and payloads without side effects`(): List<DynamicTest> {

        val cases = listOf(
            0 to emptyMap(),
            0 to mapOf("type" to "rollback"),
            0 to mapOf("type" to "exec-action", "txnId" to id),
            0 to mapOf("type" to "unknown", "txnId" to id),
            3 to emptyMap(),
            3 to mapOf("type" to "   "),
            3 to mapOf("type" to "unknown"),
            3 to mapOf("type" to "rollback")
        )
        return cases.mapIndexed { index, (version, headers) ->
            DynamicTest.dynamicTest("malformed request $index") {
                val wire = TxnWireHarness(version)
                val request = wire.Request().also {
                    it.wireVersion = version
                    it.headerData.values.putAll(headers)
                    if (headers["type"] == "unknown") it.requestBody.writeDto(emptyMap<String, String>())
                }
                AuthContext.runAsSystem { assertThatThrownBy { request.dispatch() }.isInstanceOf(Exception::class.java) }
                assertThat(wire.calls).isEmpty()
            }
        }
    }
}
