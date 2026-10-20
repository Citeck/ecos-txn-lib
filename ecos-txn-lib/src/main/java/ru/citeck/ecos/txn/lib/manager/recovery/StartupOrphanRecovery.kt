package ru.citeck.ecos.txn.lib.manager.recovery

import io.github.oshai.kotlinlogging.KotlinLogging
import ru.citeck.ecos.txn.lib.commit.repo.JournalPresence
import ru.citeck.ecos.txn.lib.transaction.TxnId
import java.time.Duration
import java.time.Instant

/**
 * One successful sweep per startup. Uncertainty retries after another minute;
 * age alone never decides an XA outcome. The app-ready job supplies the clock.
 */
internal class StartupOrphanRecovery(
    private val recovery: RecoveryManagerImpl,
    private val canAttempt: () -> Boolean = { true },
    private val isTransactionActive: (TxnId) -> Boolean = { false },
    private val journalPresence: (TxnId) -> JournalPresence
) {

    companion object {
        private val DELAY = Duration.ofMinutes(1)
        private val MIN_AGE = Duration.ofDays(30)
        private val log = KotlinLogging.logger {}
    }

    private var nextAttempt: Instant? = null
    private var complete = false

    fun start(now: Instant) {
        if (nextAttempt == null) {
            nextAttempt = now.plus(DELAY)
        }
    }

    fun runIfDue(now: Instant) {

        val next = nextAttempt ?: return
        if (complete || now.isBefore(next)) {
            return
        }
        try {
            complete = canAttempt() && recovery.rollbackOldOrphans(now.minus(MIN_AGE), journalPresence, isTransactionActive)
            if (complete) {
                log.info { "Startup prepared XA orphan sweep completed" }
            } else {
                log.warn { "Startup prepared XA orphan sweep postponed: journal lookup is uncertain" }
            }
        } catch (error: Exception) {
            log.warn(error) { "Startup prepared XA orphan sweep failed; retry in one minute" }
        } finally {
            nextAttempt = now.plus(DELAY)
        }
    }
}
