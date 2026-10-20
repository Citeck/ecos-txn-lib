package ru.citeck.ecos.txn.lib.commit.repo

/**
 * Opt-in proof that getRecoveryData queries the complete authoritative journal
 * for a global transaction, without status, owner or pagination exclusions.
 * Errors must propagate. Legacy and Noop repositories cannot assert absence.
 */
interface AuthoritativeTwoPhaseCommitRepo : TwoPhaseCommitRepo
