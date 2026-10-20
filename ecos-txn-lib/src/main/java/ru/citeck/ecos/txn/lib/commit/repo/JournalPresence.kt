package ru.citeck.ecos.txn.lib.commit.repo

/**
 * Only ABSENT is definitive permission for the startup orphan policy.
 * Missing capability, authority or a failed lookup must remain UNKNOWN.
 */
enum class JournalPresence {
    UNKNOWN,
    PRESENT,
    ABSENT
}
