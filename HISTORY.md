# History

## 0.0.1 (2021-07-26)

* Add `Service`, `CommandService` and `QueryService`.
* First release on PyPI.

## 0.0.2 (2021-09-02)

* Automatically generate the `Get{aggregate_name}s` and `Get{aggregate_name}` endpoints on the `QueryService` class.
* Implement `__get_enroute__` class method to extract the `Enroute` decorators from the methods defined on the `Service`.
* Prevent sagas for `AggregateRef` resolving to be executed when there are not any references on the corresponding `AggregateDiff`.
