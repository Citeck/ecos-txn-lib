package ru.citeck.ecos.txn.lib.transaction

import ecos.com.fasterxml.jackson210.annotation.JsonCreator
import ecos.com.fasterxml.jackson210.annotation.JsonValue
import ru.citeck.ecos.commons.utils.NameUtils
import java.time.Instant
import java.util.concurrent.atomic.AtomicLong

class TxnId private constructor(
    val version: Int,
    /**
     * Time when transaction was created
     */
    val created: Instant,
    val appName: String,
    val appInstanceId: String,
    /**
     * Unique index in application instance
     */
    val index: Long
) {

    companion object {

        private const val CURRENT_VERSION = 0

        @JvmField
        val EMPTY = TxnId(CURRENT_VERSION, Instant.EPOCH, "", "", 0L)

        private const val ID_PARTS_DELIM = ":"
        private val NAME_ESCAPER = NameUtils.getEscaperWithAllowedChars("-.")
        private const val NUM_TO_STR_RADIX = 36

        private val idCounter = AtomicLong(0)

        @JvmStatic
        @JsonCreator
        fun valueOf(text: String?): TxnId {
            if (text.isNullOrBlank()) {
                return EMPTY
            }
            val parts = NAME_ESCAPER.unescape(text).split(ID_PARTS_DELIM)
            if (parts.size != 5) {
                error("Invalid txn id: '$text'")
            }
            if (parts[0] != "0") {
                error("Unsupported txnId version: ${parts[0]}")
            }
            return TxnId(
                parts[0].toInt(NUM_TO_STR_RADIX),
                Instant.ofEpochMilli(parts[1].toLong(NUM_TO_STR_RADIX)),
                parts[2],
                parts[3],
                parts[4].toLong(NUM_TO_STR_RADIX)
            )
        }

        @JvmStatic
        fun create(appName: String, appInstanceId: String): TxnId {
            return create(appName, appInstanceId, Instant.now())
        }

        @JvmStatic
        fun create(appName: String, appInstanceId: String, created: Instant): TxnId {
            return TxnId(CURRENT_VERSION, created, appName, appInstanceId, idCounter.getAndIncrement())
        }
    }

    @JsonValue
    override fun toString(): String {
        if (appName.isEmpty()) {
            return ""
        }
        return version.toString(NUM_TO_STR_RADIX) +
            ID_PARTS_DELIM + created.toEpochMilli().toString(NUM_TO_STR_RADIX) +
            ID_PARTS_DELIM + NAME_ESCAPER.escape(appName) +
            ID_PARTS_DELIM + NAME_ESCAPER.escape(appInstanceId) +
            ID_PARTS_DELIM + index.toString(NUM_TO_STR_RADIX)
    }

    fun isEmpty(): Boolean {
        return this === EMPTY
    }

    fun isNotEmpty(): Boolean {
        return !isEmpty()
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) {
            return true
        }
        if (javaClass != other?.javaClass) {
            return false
        }

        other as TxnId

        if (version != other.version) {
            return false
        }
        if (appName != other.appName) {
            return false
        }
        if (appInstanceId != other.appInstanceId) {
            return false
        }
        if (created.toEpochMilli() != other.created.toEpochMilli()) {
            return false
        }
        if (index != other.index) {
            return false
        }
        return true
    }

    override fun hashCode(): Int {
        var result = version.hashCode()
        result = 31 * result + appName.hashCode()
        result = 31 * result + appInstanceId.hashCode()
        result = 31 * result + created.toEpochMilli().hashCode()
        result = 31 * result + index.hashCode()
        return result
    }
}
