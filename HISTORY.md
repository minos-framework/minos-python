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

## 0.0.7 (2021-11-08)

* Add `minos-microservice-common>=0.2.0` compatibility.
