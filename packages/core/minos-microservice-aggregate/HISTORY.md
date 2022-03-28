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

## 0.2.3 (2022-01-10)

* Remove the `data` wrapper from the `ModelRef`'s avro serialization.
* Add support for search by `ModelRef` instances on the `PostgreSqlSnapshot`.
* Fix bug related with the `IS_REPOSITORY_SERIALIZATION_CONTEXT_VAR`, used to store on the repository only the references to another aggregates.
* Fix bug related with concurrent enabling of the `uuid-ossp` extension from `postgres`.

## 0.2.4 (2022-01-11)

* Remove the `data` wrapper from the `IncrementalSet`'s avro serialization.
* Remove the `data` wrapper from the `ValueObjectSet`'s avro serialization.
* Remove the `data` wrapper from the `EntitySet`'s avro serialization.

## 0.4.0 (2022-01-27)

* Be compatible with `minos-microservice-common~=0.4.0`.
* Be compatible with `minos-microservice-networks~=0.4.0`.

## 0.4.1 (2022-01-31)

* Update `README.md`.


## 0.5.0 (2022-02-03)

* Rename `Aggregate` as `RootEntity`.
* Rename `AggregateRef` as `ExternalEntity`.
* Rename `ModelRef` as `Ref`.
* Rename `AggregateDiff` as `Event`.
* Create the `Aggregate` base class, with the purpose to move the business logic from the `minos.cqrs.CommandService` to this brand-new class.
* Refactor internal module hierarchy.
* Minor changes.

## 0.5.1 (2022-02-03)

* Fix bug related with dependency specification.

## 0.5.2 (2022-02-08)

* Add `Condition.LIKE` operator to be used with the `find` method from `SnapshotRepository`.
* Add `get_all` method to `RootEntity` and `SnapshotRepository` to get all the stored instance on the repository. 
* Rename `SnapshotService` command topics to avoid collisions with application-level topics. 
* Rename `TransactionService` command topics to avoid collisions with application-level topics. 
* Minor changes.

0.5.3 (2022-03-04)
------------------

* Add `RefException` to be raised when some reference cannot be resolved.
* Improve attribute and item accessors of `Ref`, `Event` and `FieldDiffContainer`
* Deprecate `Event.get_one` in favor of `Event.get_field`.
* Deprecate `Event.get_all` in favor of `Event.get_fields`.

0.5.4 (2022-03-07)
------------------

* Fix bug related with `Ref.resolve`.
* Add `RefResolver.build_topic_name` static method.
* Remove `SnapshotService.__get_one__` method.
* Minor changes.

0.6.0 (2022-03-28)
------------------

* Replace `dependency-injector`'s injection classes by the ones provided by the `minos.common.injections` module.
* Be compatible with latest `minos.common.Config` API.