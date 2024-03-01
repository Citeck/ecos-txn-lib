package ru.citeck.ecos.txn.lib.commit.repo

enum class TwoPhaseCommitStatus {
    PREPARING,
    COMMITTING,
    ROLLING_BACK
}
