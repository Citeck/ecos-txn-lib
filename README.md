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
