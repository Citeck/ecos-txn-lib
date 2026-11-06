# Independent transaction ancestry

`TransactionImpl.nesting` records the immediate parent transaction ID and the
depth of independent transactions. `TxnNesting.of(transaction)` returns this
metadata without adding management operations to the public `Transaction` API.

A root has an empty parent and depth zero. `REQUIRED` and ordinary remote calls
retain the current identity and depth. `REQUIRES_NEW` captures an active current
transaction as its parent and increments depth. This also applies while a
completed transaction remains current: its completion callbacks may still own a
connection, and a suspended ancestor can retain another. Only an absent current
transaction starts a new root at depth zero.
The previous current transaction is restored when the child scope exits.

The logical nesting limit is 64. This is separate from `txnLevel`, which limits
chains of lifecycle actions. Parent ID and depth must agree, a transaction cannot
be its own parent when created. Existing participants keep their stored ancestry
on repeated requests, including read-only to writable replacement. Only the parent ID is retained, avoiding a strong reference chain to
completed transaction objects.

Transport integrations pass received metadata as the explicit `nesting` argument
of `doInExtTxn`. The argument is mandatory and non-null; the transport maps a
legacy sender's absent metadata to `TxnNesting.ROOT`. No incoming-metadata
ThreadLocal or additional overload is needed. New external participants capture
it once; repeated requests do not validate or replace stored ancestry.
Ancestry stored in a transaction is not automatically serialized by `getData`.

An action failure before resource/participant/action registration, and before
commit coordination starts, disposes the empty transaction locally. It does not
invoke the distributed rollback journal. Otherwise a journal connection failure
could recursively start another journal transaction. Participating transactions
continue through the existing rollback and recovery protocol.

Tests cover nested creation/restoration, cross-thread participant reuse,
conflicting metadata and empty-resource failure. Datasource overflow allocation
and wire metadata are implemented in `ecos-webapp-commons`; ancestry alone does
not grant a JDBC connection or change XA commit semantics.
