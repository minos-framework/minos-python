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
