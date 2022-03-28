# History

## 0.0.1 (2021-07-26)

* Add `Service`, `CommandService` and `QueryService`.
* First release on PyPI.

## 0.0.2 (2021-09-02)

* Automatically generate the `Get{aggregate_name}s` and `Get{aggregate_name}` endpoints on the `QueryService` class.
* Implement `__get_enroute__` class method to extract the `Enroute` decorators from the methods defined on the `Service`.
* Prevent sagas for `AggregateRef` resolving to be executed when there are not any references on the corresponding `AggregateDiff`.

## 0.0.3 (2021-09-20)

* Add support for `minos-microservice-common>=0.1.13`.

## 0.0.4 (2021-09-27)

* Support `event` handling on `CommandService` inherited classes.
* Parameterize `ModelRef` reference resolution.
* Fix troubles related with dependency injections.

## 0.0.5 (2021-10-04)

* Now `ModelRef` resolving is ignored for non-`AggregateDiff` events.

## 0.0.6 (2021-10-08)

* Be compatible with the `^0.1.0` version of `minos-microservice-saga`.

## 0.0.7 (2021-10-19)

* Be compatible with the `^0.1.1` version of `minos-microservice-saga`.

## 0.1.0 (2021-11-08)

* Add `minos-microservice-common>=0.2.0` compatibility.

## 0.2.0 (2021-11-15)

* Migrate "Get Aggregate" handler functions (previously defined on `CommandService`) to `minos.aggregate.SnapshotService`.
* Add compatibility with `minos-microservice-common^=0.3.0`
* Add compatibility with `minos-microservice-networks^=0.2.0`
* Add compatibility with `minos-microservice-saga^=0.3.0`
* Add compatibility with `minos-microservice-aggregate^=0.2.0`

## 0.2.1 (2021-11-23)

* Start using the `minos.aggregate.ModelRefExtractor` for automatic `ModelRef` resolution of *Domain Events*.
* Add compatibility with `minos-microservice-networks^=0.3.0`.
* Remove `minos-microservice-saga` dependency.

## 0.4.0 (2022-01-27)

* Be compatible with `minos-microservice-common~=0.4.0`.
* Be compatible with `minos-microservice-aggregate~=0.4.0`.
* Be compatible with `minos-microservice-networks~=0.4.0`.

# 0.4.1 (2022-01-31)

* Update `README.md`.

# 0.5.0 (2022-02-03)

* Minor changes.

## 0.5.1 (2022-02-03)

* Fix bug related with dependency specification.

## 0.5.3 (2022-03-04)

* Update the `resolve_references: bool` default value to `False` defined at `PreEventHandler.handle`.
* Improve error messages of  `PreEventHandler`.

## 0.6.0 (2022-03-28)

* Replace `dependency-injector`'s injection classes by the ones provided by the `minos.common.injections` module.
* Be compatible with latest `minos.common.Config` API.
