# History

## 0.1.0 (2021-11-08)

* Migrate Aggregate-related stuff from `minos.common` to `minos.aggregate`.
* Add `TransactionRepository`.
* Refactor `ModelRef` to be used as a full class instead of a simple type hint label.
* Rename `MinosRepository` as `EventRepository`.
* Rename `MinosSnapshot` as `SnapshotRepository`.

## 0.1.1 (2021-11-09)

* Do not raise a `TransactionRepositoryConflictException` exception when `TransactionStatus.PENDING` status do not change.
* Add `TransactionRepository.get(uuid: UUID)` method
* Clean tests.

## 0.2.0 (2021-11-15)

* Add `SnapshotService` containing `Get${AGGREGATE_NAME}`, `Get${AGGREGATE_NAME}s` and the periodic event to synchronize the snapshot if reads are received for a long time.
* Add `TransactionService` containing `Reserve${MICROSERVICE_NAME}Transaction`, `Reject${MICROSERVICE_NAME}Transaction` and `Commit{MICROSERVICE_NAME}Transaction`.
* Be compatible with `minos-microservice-common=^0.3.0`.
* Add `minos-microservice-networks` as dependency.

## 0.2.1 (2021-11-16)

* Add periodic event handler on `TransactionService` to reject `TransactionEntry` instances that are blocked (for more than a minute on `TransactionStatus.RESERVED` status).
* Add ordering capabilities to `AggregateDiff` instances.
* Add `updated_at` filter into `TransactionRepository.select`

## 0.2.2 (2021-11-22)

* Add `ModelRef.resolve(...)` method to resolve `ModelRef` instances.
* Add `ModelRefResolver` to resolve `ModelRef` instances in batch.
* Fix bug that published *Domain Events* when non-top-level transactions are committed.
