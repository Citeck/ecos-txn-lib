# Transaction Library

## Overview

This library provides a robust, distributed transaction management system, built for managing complex transactional workflows. 
It supports both two-phase commit (2PC) and one-phase commit processes, ensuring consistency and reliability across multiple 
resources and services. With an emphasis on flexibility and fault tolerance, the library is designed to handle distributed 
transactions efficiently, even in the event of system failures.

## Key Features

- **Distributed Transaction Coordination**: Manage transactions across multiple services or systems, ensuring consistency with the two-phase commit (2PC) protocol.
- **Flexible Transaction Lifecycle Management**: Start, commit, or roll back transactions with fine-grained control, adapting to various transactional needs.
- **Fault Tolerance and Recovery**: Automatic recovery mechanisms allow transactions to continue after failures, ensuring data consistency even in distributed systems.
- **Read-Only Transactions**: Support for read-only transactions, providing lightweight operations where data consistency checks are needed without performing writes.
- **Action Scheduling**: Schedule actions to be executed before or after transactions, enabling sophisticated workflows that align with your business logic.
- **Resource Management**: Efficiently manage transaction resources, with support for transactional resource registration, preparation, and disposal.

## Use Cases

- **Distributed Microservices**: Coordinate transactions between microservices, ensuring data integrity across independent services.
- **Database and External System Integration**: Commit or roll back changes across databases, message queues, or other transactional resources in a consistent manner.
- **Fault Recovery**: Continue interrupted transactions after a failure, with built-in recovery and rollback mechanisms.
- **Custom Workflows**: Use scheduled actions to implement custom workflows, such as audit logging or notifications, that are triggered by transactional events.

## Read-only caches

Use `txn.getReadOnlyCache<K, V>(key)` for results derived from mutable data. Calls with
an equal key share a map while the transaction stays read-only. Returning to writable
mode clears these maps, including when a nested read-only action restores its outer
writable mode. Disposal also clears them. In writable mode, the method returns a
fresh map that the transaction does not retain.

```kotlin
TxnContext.doInTxn(readOnly = true) {
    val cache = TxnContext.getReadOnlyCache<String, Result>(cacheKey)
    val result = cache.getOrPut(recordId) { loadResult(recordId) }
}
```

`TxnContext.getReadOnlyCache<K, V>(key)` delegates to the current transaction. If no
transaction exists, it returns a fresh empty map on each call without starting a transaction.

Do not retain cache maps across action scopes. Keep transaction resources, deferred
actions, accumulated changes and other lifetime state in `getData`; read-only cache
invalidation does not clear that state.

## Commit decisions and cleanup

After coordination starts, its coordinator owns commit/rollback decisions. An
exception from a phase-two request or a journal write does not authorize the
originator to roll back participants. Local coordination errors are queued for
recovery, which rereads the stored decision; a lost `beforeCommit` response can
represent either PREPARING or COMMITTING. Failures before that write is attempted
still release active resources through rollback, even if recording the rollback
fails. Delegation records its destination before sending the request.

A failed delegation request does not tell whether the coordinator received it. The
coordinator cannot decide COMMIT without the prepare vote of the originator's branch, so
while that branch has not started to prepare, the originator rolls it back and then rolls
back every participant: rollback is the only possible outcome. The originator's branch is
rolled back before any participant, otherwise the coordinator could prepare it in between.
A branch that started to prepare, or is already gone because the coordinator completed and
disposed it, stays with the coordinator; the original error is rethrown in every case. An
originator without its own branch has no vote. It only releases its context, so participants
asking it about the transaction get NO_TRANSACTION and their inactivity job releases unprepared
branches. Only upgraded originators follow these rules: an older one keeps its branch open
while the coordinator reports its own participant branch as active.

A branch rolled back before it voted leaves a process-local tombstone for an hour (at most
100,000 transactions), and a later prepare of that transaction fails. Without it, the web
layer would recreate an empty context for the late request, which would vote "nothing to
commit" and let the coordinator commit the other branches. The tombstone is written in the
same critical section as the rollback, before the context can be disposed. A prepare looks
its context up before consulting the tombstone, so a context recreated after such an abort
is refused. The inactivity job takes the same path for unprepared non-empty writable
branches; a branch prepared after the job checked its status stays with the coordinator.
Tombstones do not survive a restart: a late prepare that reaches a restarted app still finds
no branch and votes "nothing to commit". Telling that answer apart from a genuine empty vote
requires a coordinator-side protocol change.

A read-only root makes no commit decision, but its participants keep their branches, and
with them their database connections, until the root tells them to release. When the
root's action returns or throws, the root disposes its own branch first and then sends a
dispose to every participant that registered in the transaction, one `txn-actions` task
per participant, without touching the 2PC journal. A notification that fails or cannot be
scheduled is only logged and never replaces the root's result or error. Such a participant,
like one the root never heard from (its response was lost), is released by its inactivity
job once the branch has been idle for 20 seconds and the root answers NO_TRANSACTION. A
failed local dispose of the root does not stop the notification. The notification runs on a pool thread without the root
transaction, so the web layer does not pin it to the replica that served the root's calls:
with several replicas of a participant it can reach another one, where it is a no-op, and
the inactivity job releases the branch.

Completion calls (prepare, commit, rollback, dispose) are serialized on the transaction's
monitor, because coordinator requests, the originator and the inactivity job make them on
different threads. Synchronization callbacks of those calls run under the monitor.

A partially committed two-phase transaction stays COMMITTING. Recovery retries
only unfinished prepared resources; successful and read-only resources are not
committed again. Explicit rollback and disposal cannot reverse COMMITTING, and
failed disposal retains the manager's live transaction entry. The inactivity
job preserves unresolved coordinator-owned transactions. Completed contexts can
release their resources after transferring their outcome to the recovery manager's
bounded process-local receipts, so a lost acknowledgement remains retryable. Rollback acknowledgements are also journaled
before successful participant contexts are disposed; failed or uncertain rollbacks
remain available for retry. Completion actions should be idempotent because retries
can repeat their dispatch after a lost acknowledgement.
Without a durable coordinator repository, unresolved commits require explicit
reconciliation; a failed one-phase commit is never replayed as a two-phase commit.

Register transaction actions and synchronizations in the transaction's serialized
execution flow. Concurrent registration from another thread during completion is
not supported. Synchronization flush actions can make their originator an
actions-only participant, selecting 2PC even when only one remote app has XA
resources. This preserves callback completion; an optimization must retain that
completion before omitting such a participant.

## Synchronization and recovery safety

Writable transactions schedule `beforeCompletion` through `BEFORE_COMMIT`, after
ordinary ordered actions. The root drains newly registered before-commit actions
before it snapshots participants and writes the 2PC journal. Action ordering is
re-evaluated after every action, so newly queued ordinary writes run before a
pending synchronization flush. A synchronization may
enlist resources in that phase; each registered callback runs once. Resource and
remote-Xid enlistment is sealed when `PREPARING` or `COMMITTING` starts. Direct
management callers that bypass the action phase may flush existing resources in
the fallback callback, but cannot add resources there.

`JavaXaTxnManagerAdapter` is also a `TransactionSynchronizationRegistry`; wire one
instance to a managed pool as both its transaction manager and its registry. Interposed
synchronizations of a transaction share one ECOS synchronization, so a pool that
registers one per borrowed connection costs the root one remote action per participant. A synchronization
registered after that batch ran `beforeCompletion` opens a new batch, which still runs
before the commit. Synchronizations registered with the JTA transaction directly keep an
action each. Registry calls outside an active transaction fail with
`IllegalStateException`.

Recovery scans registered storages before changing any branch. It completes all
visible branches of each requested global transaction, including late branches
omitted from an older participant's journal snapshot. Other global transactions
are unaffected. Storage scans must expose all prepared branches available to the
application; scan errors propagate to the coordinator.

After every registered storage has been enumerated successfully, an absent Xid is
an idempotent completion: a participant can have committed before its reply was lost
and then restarted. The same durable commit/rollback decision can be acknowledged
without replaying application writes. An empty storage registry for explicit nonempty branch IDs without a matching
receipt still fails; connection/enumeration failures propagate and retain the journal.
The webapp integration initializes every configured XA datasource before exposing
transaction RPCs and starting background recovery. Custom integrations must establish
the same registration invariant. Datasource keys are not persisted in the Xid or 2PC
journal; source configuration must continue to identify the databases holding the
prepared branches across restart.

Successful calls and disposed live outcomes are remembered for up to 10,000
process-local receipts, including global completion markers. These reject known
opposite decisions and support partial retries, but are not required to acknowledge
an already completed branch after restart. Recovery never changes a durable COMMIT
to rollback. A full scan cannot diagnose a removed or redirected datasource or an
external heuristic outcome; such configuration/administrative changes require separate
reconciliation rather than interpreting the journal as a replay of application work.

Recovery API version 3 also scans a participant whose journal Xid set is empty,
using an empty-branch global-transaction marker. Coordinators retain the journal
when a participant does not support version 3; older recovery APIs can silently
acknowledge branches they did not discover. Upgrade coordinators as well as
participants before relying on this repair. The earlier callback phase and
enlistment guard apply only to upgraded participants; a mixed-version deployment
cannot guarantee complete participant discovery from old code. An action-only
participant with an empty snapshot and no XA sources acknowledges its global marker
after restart; an explicit resource branch still requires a registered source.


## Startup cleanup of old prepared XA branches

Once per application startup, the app-ready background job waits one minute and scans
registered `RecoverableStorage` sources. It considers only global `TxnId.created` dates
strictly older than 30 days. Recent, future, empty or invalid identifiers are left alone.

The explicitly configured `EcosTxnProps.commitCoordinatorApp` is queried through
transaction API v3 (`ecos.webapp.txn.commitCoordinatorApp` in the Spring integration).
An empty coordinator configuration cannot prove authority and returns UNKNOWN, including
for transactions originating locally. To use a local authoritative repository, explicitly
configure this application's name as coordinator. The originating app encoded in a TxnId
is not assumed to own the journal.
Any existing journal record, including COMMITTING, leaves the branches to ordinary
recovery. Only a successful explicit ABSENT answer permits rollback under this old-orphan
policy. A live local transaction or known committed completion receipt also prevents rollback.

Repository providers must opt in with `AuthoritativeTwoPhaseCommitRepo`: their
`getRecoveryData` must search the complete journal by global ID across all statuses and
owner instances, and throw on read failures. The emodel repository implements this
contract. Noop and legacy repository providers cannot prove absence. Mixed deployments
require API v3 on the coordinator; legacy participants continue using their existing APIs.

Unavailable coordinator, unsupported API, UNKNOWN answer, enumeration/read/rollback
failure postpone the unfinished sweep for another minute. Every retry checks the journal
again for remaining prepared branches. A successful sweep ends startup cleanup until the
next restart; this is not a periodic age-based recovery rule. It covers only registered
sources and does not infer historical XA decisions from age or from an empty journal
response of an unqualified repository. Changing coordinator ownership or database scope
requires operator reconciliation before using that new scope as authoritative for old branches.

Journal lookups run outside the recovery monitor so a slow coordinator cannot hold up
unrelated completion receipts or ordinary recovery. Before rollback, the monitor is
reacquired, prepared branches are refreshed, and COMMIT receipts and live local contexts
are checked again. A refresh failure remains uncertain and schedules a retry.

Before enumeration, a local preflight checks explicit coordinator configuration,
availability and API capability. An uncertain preflight postpones the attempt without
scanning sources. This is not journal absence proof: each candidate still needs its own
version-checked authoritative lookup. The JDBC adapter treats XAER_NOTA after a racing
completion or another datasource alias as idempotent absence; other XA failures propagate.

Global branch recovery and authoritative journal lookup are introduced together in
transaction API v3. API v2 does not provide these capabilities.

## Test coverage

Run the complete library suite with `mvn clean verify` on JDK21. The wire protocol tests
exercise the real transaction client, executor, action dispatcher and JSON serializers
over an in-memory byte transport, covering published legacy0/1 and structured2/3
protocols, system-only authorization, malformed requests and transport failures.
Negotiated legacyv1 remains v1 at both client and dispatcher so prepare ownership is
preserved; unavailable API negotiation fails before request dispatch. Transaction manager
calls are recorded at the transport boundary; lifecycle/resource tests remain separate.
These tests do not replace real PostgreSQL/XA failure injection or deployment checks.

The suite also exercises these contracts:

| Component | Scenarios |
| --- | --- |
| XA/JTA adapters | All status mappings, transaction wrapper lifetime, identity-based enlistment, rollback synchronization, original XAException propagation, registry key and resources per transaction and registration refused outside an active transaction |
| Interposed synchronizations | One remote action for all interposed synchronizations of a participant, registration during and after the batch observed against resource completion, batches shared by calls and pools of one transaction, direct registrations kept separate, rollback on a failed callback and isolation of a failed afterCompletion |
| Java facade | Java-compiled Runnable/Supplier/Consumer overloads, checked exceptions, read-only and new-transaction scopes, parent/MDC restoration, batching and no-manager fallback |
| Policies and actions | External transaction policy routing, scope restoration on failure, continued callbacks after an action fails and rollback error suppression |
| Health job | Busy/recent/expired contexts, exact owner/delegated routes, unavailable or failing status lookup, definitive missing owner and retry after disposal failure |
| Coordinator recovery | Bounded own/global scans, live-context exclusions, instance routes, completion actions, partial acknowledgements and journal failures |
| Delegation failure | Lost and delayed coordination requests over web-layer routing, prepared and missing originator branches, originator without a branch, tombstoned late prepares, abort racing with prepare and with the inactivity job, dispose waiting for the tombstone of a running abort |
| Read-only root completion | Participant branches released after a returned and after a failed root, one notification per participant after the local dispose, the root's error kept when a participant cannot be notified, the executor rejects the task or the local dispose fails, nothing scheduled without remote participants, participants unknown to the root left to the inactivity job |
| Xid serialization | JSON/CBOR binary branches and empty recovery markers; malformed types, format integer overflow and extra tuple members are rejected |

Health decisions run directly with controlled activity timestamps; the tests do not wait
for minute-long timers. The daemon's real scheduling, shutdown hooks, HTTP middleware
and database crash/restart behavior require separate integration checks. XA prepare
votes cover the specified `XA_OK` and `XA_RDONLY` values; invalid provider votes are
outside that interface contract. Test helpers do not certify a particular JDBC driver.
The Java MDC check temporarily installs an in-memory MDC adapter and restores the
original binding in `finally`, because the test logging binding has no MDC storage.

When maintaining tests, give each scenario a distinct contract and observation boundary.
A wrapper test should verify the exact target, transaction ID, payload and returned result,
using nonempty payloads and nonzero action IDs/levels. A call-only assertion cannot prove
argument forwarding. Negative call assertions must match the recorder's full encoding.
Recovery tests need an actual branch outcome or acknowledgement attempt; an unchanged
journal alone can also mean recovery never ran. Callback ordering checks must observe the
resource completion event. Do not retain separate getter smoke tests when a stronger
lifecycle case already observes the same contract. Similar scenarios at Java, manager,
resource and wire boundaries may remain when they detect different failures.

A local stand check also ran the current library against PostgreSQL 18.6 and pgjdbc
42.7.13 using separate participant/coordinator JVMs, real XA through the production
JDBC factory, HTTP through the transaction client/executor, and the actual emodel
journal repository backed by SQL. It verified normal 2PC, a coordinator crash after
persisting COMMITTING followed by all-process restart, and participant failure after
prepare before its reply. Repeated recovery caused no duplicate effects.

The follow-up fix restores idempotent completion after a participant commits, loses
its reply and restarts: the coordinator removes COMMITTING after successful recovery,
with both database effects present exactly once. The harness now invokes the actual
EcosDataSourceManager/EcosTxnInitializer bootstrap. All configured XA sources are
registered before any application lookup or transaction endpoint. Each participant
also configures an alias to the same database; XAER_NOTA after another request or alias
completed a branch is idempotent, while resource and heuristic errors propagate.

Startup checks cover authoritative ABSENT/PRESENT, coordinator unavailable, lookup
HTTP errors, negotiated APIv2, live/young transactions, strict 30-day boundary and
one successful sweep per startup. Enumeration errors preserve prepared branches and
allow retries. Ordinary recovery with an artificially cleared registry or a failed
source scan retains the unacknowledged participant in the SQL journal; restoring the
source completes it once. Missing Xid after a successful complete scan is acknowledged.

Task `ecos-txn-lib-local-stand-20261020` retains harness, source/class provenance and
SQL/XA observations. Current worker readiness callbacks are explicitly disabled and
the absence of a transaction-manager daemon is asserted at startup; controlled clocks
exercise the real sweep without claiming natural daemon scheduling, shared Spring/JWT
middleware or ZooKeeper/distributed-lock certification. Earlier reports describe the
pre-fix receipt limitation and an older harness readiness assumption, superseded by
the task's follow-up evidence. No release or deployment is implied by local checks.
