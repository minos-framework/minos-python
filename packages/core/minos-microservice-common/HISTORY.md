History
==========

0.0.1.1-alpha (2021-03-28)
--------------------------------

0.0.1.2-alpha (2021-03-31)
-----------------------------

0.0.1.3-alpha (2021-03-31)
----------------------------

0.0.1.4-alpha (2021-04-02)
------------------------------

0.0.1.5-alpha (2021-04-02)
----------------------------

0.0.1.6 (2021-04-03)
---------------------

0.0.1.7 (2021-04-06)
----------------------

0.0.2 (2021-04-19)
-------------------

0.0.3 (2021-04-26)
--------------------

0.0.4 (2021-04-28)
--------------------

0.0.5 (2021-05-03)
--------------------

0.0.6 (2021-05-04)
--------------------

0.0.7 (2021-05-06)
--------------------

0.0.8 (2021-05-07)
--------------------

0.0.9 (2021-05-10)
-------------------

0.0.10 (2021-05-11)
---------------------

0.0.11 (2021-05-12)
---------------------

0.0.12 (2021-05-17)
---------------------

0.0.13 (2021-05-18)
---------------------

0.0.14 (2021-05-20)
--------------------

0.0.15 (2021-05-26)
--------------------

* Some code refactoring
* Test cases coverage optimization
* fixed some Sagas functionalities
* fixed Sphinx documentation generation process

0.0.16 (2021-05-28)
--------------------

0.0.17 (2021-06-02)
--------------------

* Documentation improvements
* Removed DependencyInjector, conflict with minos.microservice package
* Removed old module messages
* Added abstract classes Response and Request for Microservice Controller
* Bugfix

0.1.0 (2021-06-18)
--------------------

* Enrich exception messages
* Classes refactors
* Added support for DTO Model class

0.1.1 (2021-07-01)
--------------------

* Resolved float problem with avro
* Bugfixes

0.1.2 (2021-07-06)
--------------------

* Added Exceptions Traitment
* Bugfixes
* Creates CommandStatus for SAGA Reply
* Improved queries for Aggregate ( per id query )
* Added methods for Snapshot ( added check for duplicates )

0.1.3 (2021-07-12)
--------------------

* AggregateRef Model
* ValueObjects Model
* Bugfixes
* Removed ID to UUID for models
* Events use AggregateDIff

0.1.4 (2021-07-19)
--------------------

* Bugfixes

0.1.5 (2021-07-19)
--------------------

* Bugfixes

0.1.6 (2021-07-21)
--------------------

* Bugfixes
* Added Pool for Kafka

0.1.7 (2021-07-27)
--------------------

* Simplify configuration file
* Bugfixes

0.1.8 (2021-08-03)
--------------------

* Connection Pool improvement for async
* Small inmprovements
* Bugfixes

0.1.9 (2021-08-17)
--------------------

* Add `minos.common.Entity`.
* Refactor `minos.common.AggregateDiff.fields_diff`:
  * Support incremental field differences over `EntitySet` and `ValueObjectSet`.
  * Add `FieldDiff`, `IncrementalFieldDiff` and `FieldDiffContainer`.
* Now `Model` inherits from `collections.abc.Mapping`.
* Add support for `typing.Generic` and `typing.TypeVar`.
* Bugfixes

0.1.10 (2021-08-23)
--------------------

* Add `created_at` and `updated_at` to `Aggregate`.
* Improve Fix `AggregateDiff` + `FieldDiffContainer` API.
* Fix `Aggregate` bug that stored empty field differences instead of skipping them.
* Fix bug from `Model` related with `getitem`, `setitem`, `getattr` and `setattr`.

0.1.11 (2021-08-25)
--------------------

* Fix bug related with `datetime` serialization in `Field`.
* Fix bug related with the `AggregateDiff.__getattr__` implementation.

0.1.12 (2021-09-01)
--------------------

* Add generics to `MinosSagaManager` interface.
* Remove `saga.items` from `MinosConfig`.
* Add `service.aggregate` to `MinosConfig`.
* Remove `interval` from `EntrypointLauncher` (must be setup at service level).
* Fix bug related with `TypeHintBuilder` and `Any`.
* Fix bug related with `ModelType` comparisons.

0.1.13 (2021-09-16)
--------------------

* Add `discovery.client` into `MinosConfig` allowing to setup custom Discovery clients.
* Add `minos.common.queries` module, containing `Condition` and `Ordering` classes.
* Refactor `PostgreSqlSnapshot` to store `Aggregate`s following a `schema` (bytes) + `data` (json) strategy supporting queries over the `data` column.
* Fix troubles related with `avro`'s `record` collisions with full patching by `AvroSchemaEncoder`.
* Refactor `AvroDataEncoder` to be more consistent with `Model` and `Field` responsibilities.
* Remove file headers.
* Stop using `NoReturn` everywhere.

0.1.14 (2021-09-27)
--------------------

* Add support for `timedelta`.
* Fix `Optional[ModelRef[T]]` behaviour.
* Remove `events` section from `MinosConfig`.
* Fix troubles related with dependency injections.

0.1.15 (2021-10-04)
--------------------

* Fix bug from `PostgreSqlSnapshotReader` that returned already deleted aggregates when `Condition.TRUE` was passed.

0.1.16 (2021-10-07)
--------------------

* Improve support for `Model` inheritance inside container classes (`list`, `dict`, `EntitySet`, etc.).
* Add support for `set[T]` type.
* Fix bug related with complex types and `PostgreSqlSnapshotQueryBuilder`.
* Fix bug related with empty `dict` and `minos.saga.SagaContext`.

0.1.17 (2021-10-08)
--------------------

* Add `IncrementalSet` as the base implementation for incremental sets.
  * Now `ValueObjectSet` and `EntitySet` inherit from `IncrementalSet`.
* Refactor `ValueObjectSet` to be implemented on top of a `set` instead of a `dict`.
* Fix bug related with `list`, `set` and `dict` in combination with `Any` and the `avro` schemas.

0.2.0 (2021-11-08)
--------------------

* Migrate all Aggregate-related stuff to the brand-new `minos.aggregate` package.
  * Move `Aggregate`, `Entity`, `ValueObject`, etc. model classes and related utility classes (`AggregateDiff`, `FieldDiff`, Action`, `ModelRef`, etc.).
  * Move `minos.common.repository` module.
  * Move `minos.common.snapshot` module.
  * Move `minos.common.queries` module.
* Add `Lock` class as an abstract class to implement application-level locking in `minos`.
  * Add `PostgreSqlLock` and `PostgreSqlLockPool`
* Replace the `MinosPool` instance creation strategy, from "create if possible or get or wait" to "get or create if possible or wait".

0.2.1 (2021-11-09)
--------------------

* Fix bug related with dependency injections over `minos.*` modules.

0.3.0 (2021-11-15)
--------------------

* Add `services` and `middleware` sections to `MinosConfig`.
* Remove `Command`, `CommandReply`, `CommandStatus` and `Event` (moved to `minos.networks`).
* Remove `MinosBroker` and `MinosHandler` (moved to `minos.networks`).
* Remove `MinosSagaManager` (moved to `minos.saga`).

0.3.1 (2021-11-24)
--------------------

* Fix bug related with `enum.Enum` inherited classes that do not recover the original type after being serialized.
* Fix bug related with`str` values auto-casting to an iterable of characters.

0.3.2 (2021-11-30)
--------------------

* Fix bug related with `EntrypointLauncher`'s dependency injection and unloaded `minos.*` modules.

0.3.3 (2022-01-10)
--------------------

* Big refactor of the `minos.common.model.serializers` module:
  * Add `encode_schema`, `decode_schema`, `encode_data` and `decode_data` callback-like functions to be able to modify the serialization/deserialization logic.
  * Add `SchemaEncoder`, `SchemaDecoder`, `DataEncoder` and `DataDecoder` abstract base classes.
  * Increase serialization/deserialization robustness.

0.3.4 (2022-01-11)
--------------------

* Add `batch_mode: bool` argument to manage if the `avro` serialization is performed for a single model or a batch of models.

0.4.0 (2022-01-27)
------------------

* Add waiting time before destroying the `minos.common.MinosPool` acquired instances.

0.4.1 (2022-01-31)
------------------

* Update `README.md`.

0.5.0 (2022-02-03)
------------------

* Minor changes.

0.5.2 (2022-02-08)
------------------

* Add `query_repository` section to `MinosConfig`.
* Minor changes.

0.5.3 (2022-03-04)
------------------

* Big performance improvement related with a caching layer over type hint comparisons at `TypeHintComparator`.
* Improve attribute and item accessors of `Model`.
* Fix bug related with casting from `dict` to `Model` instances on field setters.

0.6.0 (2022-03-28)
------------------

* Add `Config` with support for config versioning.
* Add `ConfigV1` as the class that supports the V1 config file.
* Add `ConfigV2` as the class that supports the V1 config file.
* Deprecate `MinosConfig` in favor of `Config`. 
* Add `get_internal_modules` function.
* Add `Injectable` decorator to provide a way to set a class as injectable.
* Add `InjectableMixin` class that provides the necessary methods to be injectable.
* Add `Inject` decorator to provide a way to inject variables on functions based on types.
* Add `LockPool` base class as the base class for lock pools.
* Add `Object` base class with the purpose to avoid issues related with multi-inheritance and mixins.
* Add `Port` base class as the base class for ports.
* Add `CircuitBreakerMixin` class to provide circuit breaker functionalities.
* Add `SetupMixin` class as a replacement of the `MinosSetup` class.