package ru.citeck.ecos.txn.lib.support

import ru.citeck.ecos.commons.data.DataValue
import ru.citeck.ecos.commons.json.Json
import ru.citeck.ecos.context.lib.auth.AuthContext
import ru.citeck.ecos.txn.lib.commit.repo.JournalPresence
import ru.citeck.ecos.txn.lib.manager.TransactionManager
import ru.citeck.ecos.txn.lib.manager.api.client.TxnManagerWebApiClient
import ru.citeck.ecos.txn.lib.manager.api.server.TxnManagerRemoteActions
import ru.citeck.ecos.txn.lib.manager.api.server.TxnManagerWebExecutor
import ru.citeck.ecos.txn.lib.transaction.ManagedTransaction
import ru.citeck.ecos.txn.lib.transaction.TransactionStatus
import ru.citeck.ecos.txn.lib.transaction.xid.EcosXid
import ru.citeck.ecos.webapp.api.apps.EcosRemoteWebAppsApi
import ru.citeck.ecos.webapp.api.promise.Promise
import ru.citeck.ecos.webapp.api.promise.Promises
import ru.citeck.ecos.webapp.api.web.EcosWebHeaders
import ru.citeck.ecos.webapp.api.web.body.*
import ru.citeck.ecos.webapp.api.web.client.*
import ru.citeck.ecos.webapp.api.web.executor.*
import java.io.*
import java.lang.reflect.Proxy
import java.nio.file.Path

/**
 * In-memory byte transport around the real client, executor and action dispatcher.
 * Only the manager boundary is recorded; request DTOs still pass through Jackson.
 */
internal class TxnWireHarness(var version: Int) {

    data class Call(val method: String, val args: List<Any?>)

    val calls = mutableListOf<Call>()
    val requests = mutableListOf<Request>()
    var prepared = emptyList<EcosXid>()
    var status = TransactionStatus.PREPARED
    var presence = JournalPresence.PRESENT
    var transportFailure: RuntimeException? = null
    var available = true
    val availabilityRequests = mutableListOf<String>()

    private val managed = proxy(ManagedTransaction::class.java) { name, args ->
        calls.add(Call("txn.$name", args))
        when (name) {
            "getStatus" -> status
            else -> Unit
        }
    }
    private val manager = proxy(TransactionManager::class.java) { name, args ->
        calls.add(Call(name, args))
        when (name) {
            "getManagedTransaction" -> managed
            "prepareCommitFromExtManager" -> prepared
            "getStatus" -> status
            "getJournalPresence" -> presence
            else -> Unit
        }
    }
    val executor = TxnManagerWebExecutor(TxnManagerRemoteActions(manager))
    private val web = object : EcosWebClientApi {
        override fun getApiVersion(targetApp: String, path: String, maxVersion: Int): Int {
            return if (version < 0) version else minOf(version, maxVersion)
        }
        override fun newRequest(): EcosWebClientReq {
            return Request()
        }
    }
    val client = TxnManagerWebApiClient(
        web,
        object : EcosRemoteWebAppsApi {
            override fun isAppAvailable(appName: String): Boolean {
                availabilityRequests.add(appName)
                return available
            }
        }
    )

    inner class Request : EcosWebClientReq, EcosWebExecutorReq {

        var target = ""
        var requestPath = ""
        var wireVersion = 0
        val headerData = Headers()
        val requestBody = Body()
        val responseBody = Body()

        override fun targetApp(targetApp: String): EcosWebClientReq {
            target = targetApp
            return this
        }
        override fun path(path: String): EcosWebClientReq {
            requestPath = path
            return this
        }
        override fun version(version: Int): EcosWebClientReq {
            wireVersion = version
            return this
        }
        override fun headers(headers: Any): EcosWebClientReq {
            DataValue.create(headers).forEach { key, value -> this.headerData.values[key] = value }
            return this
        }
        override fun header(key: String, value: Any?): EcosWebClientReq {
            headerData.values[key] = value
            return this
        }
        override fun body(body: (EcosWebBodyWriter) -> Unit): EcosWebClientReq {
            body(this.requestBody)
            return this
        }
        override fun <T> execute(response: (EcosWebClientResp) -> T): Promise<T> {

            requests.add(this)
            transportFailure?.let { throw it }
            AuthContext.runAsSystem { dispatch() }
            return Promises.resolve(
                response(object : EcosWebClientResp {
                    override fun getHeaders(): EcosWebHeaders {
                        return Headers()
                    }
                    override fun getBodyReader(): EcosWebBodyReader {
                        return this@Request.responseBody
                    }
                })
            )
        }
        fun dispatch() {
            executor.execute(
                this,
                object : EcosWebExecutorResp {
                    override fun setHeader(key: String, value: Any?) {}
                    override fun getBodyWriter(): EcosWebBodyWriter {
                        return responseBody
                    }
                }
            )
        }
        override fun getApiVersion(): Int {
            return wireVersion
        }
        override fun getHeaders(): EcosWebHeaders {
            return headerData
        }
        override fun getBodyReader(): EcosWebBodyReader {
            return requestBody
        }
    }

    class Headers : EcosWebHeaders {

        val values = linkedMapOf<String, Any?>()

        override fun has(key: String): Boolean {
            return key in values
        }
        override fun <T : Any> get(key: String, type: Class<T>): T? {
            return Json.mapper.convert(values[key], type)
        }
        override fun <T : Any> getOrElse(key: String, orElse: T): T {
            return Json.mapper.convert(values[key], orElse.javaClass) ?: orElse
        }
        override fun <T : Any> getAs(type: Class<out T>): T {
            return Json.mapper.convertNotNull(values, type)
        }
        override fun <T : Any> getAsOrNull(type: Class<out T>): T? {
            return Json.mapper.convert(values, type)
        }
    }

    class Body : EcosWebBodyReader, EcosWebBodyWriter {

        private val bytes = ByteArrayOutputStream()

        override fun getType(): BodyContentType {
            return BodyContentType.JSON
        }
        override fun getInputStream(): InputStream {
            return ByteArrayInputStream(bytes.toByteArray())
        }
        override fun <T : Any> readDto(type: Class<out T>): T {
            return Json.mapper.readNotNull(bytes.toByteArray(), type)
        }
        override fun <T : Any> readDtoOrNull(type: Class<out T>): T? {
            return Json.mapper.read(bytes.toByteArray(), type)
        }
        override fun start(type: BodyContentType) {}
        override fun writeText(text: String) {
            writeBytes(text.toByteArray())
        }
        override fun writeStream(stream: InputStream): Long {
            return stream.copyTo(bytes)
        }
        override fun writeBytes(bytes: ByteArray) {
            this.bytes.write(bytes)
        }
        override fun writeFile(file: File) {
            file.inputStream().use { writeStream(it) }
        }
        override fun writeFile(file: Path) {
            writeFile(file.toFile())
        }
        override fun writeDto(value: Any) {
            writeBytes(Json.mapper.toBytesNotNull(value))
        }
        override fun getOutputStream(): OutputStream {
            return bytes
        }
    }

    companion object {
        fun <T> proxy(type: Class<T>, call: (String, List<Any?>) -> Any?): T {

            val value = Proxy.newProxyInstance(type.classLoader, arrayOf(type)) { _, method, args ->
                call(method.name, args?.toList() ?: emptyList())
            }
            return type.cast(value)
        }
    }
}
