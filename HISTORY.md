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
