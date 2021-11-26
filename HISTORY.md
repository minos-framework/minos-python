History
=======

0.0.1 (2021-05-12)
------------------

* First Release
* added support for Saga Instances
* Saga Actions

0.0.2 (2021-05-14)
------------------

* Moved to Poetry
* Added actions for documentation

0.0.3 (2021-05-20)
------------------

* Added Dependency injection Container use
* Completed Saga Manager

0.0.4 (2021-05-31)
------------------

* Bugfixes

0.0.5 (2021-06-02)
------------------

* Created SagaContext class
* Documentation improvement
* implemented __getitem__ and __setitem__ for SagaContext

0.0.6 (2021-07-06)
------------------

* Completed the Saga process workflow implementation
* Bugfixes
* Completed the Compensation process
* Added Reply Command to Saga process

0.0.7 (2021-07-12)
------------------

* Added update to common
* Bugfixes

0.0.8 (2021-07-23)
------------------

* Now Sagas can be paused also on memory in addition to on disk.
* Improve SagaManager API.

0.0.9 (2021-07-26)
------------------

* Add flag to return SagaExecution instances directly from SagaManager

0.0.10 (2021-08-23)
------------------

* Support latest `minos-microservice-common` release (`0.1.10`)
* Change default `SagaManager` flags to (`pause_on_disk=False, raise_on_error=True, return_execution=True`)
* Enrich failed `CommandReply` instances with exception message.
* Add `SagaRequest` and `SagaResponse`.

0.0.11 (2021-09-01)
------------------

* Deprecate `SagaManager.run` by `Saga` names (the new approach is to directly pass the `Saga` instance).
* Remove `name` from `Saga` class.
* Minor improvements.

0.0.12 (2021-09-27)
------------------

* Fix troubles related with dependency injections.
* Remove file headers.
* Stop using `NoReturn` everywhere.

0.1.0 (2021-10-08)
------------------

* Add `SagaStep.on_error` method to handle errored responses.
* Integrate `SagaRequest` and `SagaResponse` into `SagaOperation` definitions.
* Rename `SagaStep` methods (`invoke_participant` as `on_execute`, `on_reply` as `on_success`, `with_compensation` as `on_failure`).
* Enrich `SagaRequest` and `SagaResponse` to be classes instead of simple constructor helpers.
* Add `CommitExecutor`.
* Rename `LocalExecutor` as `Executor`, `OnReplyExecutor` as `ResponseExecutor` and `PublishExecutor` as `RequestExecutor`.
* Simplify `SagaException` and heirs names.

0.1.1 (2021-10-19)
------------------

* Add `ConditionalSagaStep` and `ConditionalSagaStepExecution` which allow to define conditional logic for steps within the `SagaExecution`.
* Add `LocalSagaStep` and `LocalSagaStepExecution` classes which allow to execute local functions within the `SagaExecution`.
* Improve `SagaStep` and `SagaStepExecution` class hierarchy.
* Support dynamic `user` setup through a `ContextVar[Optional[UUID]]` variable.
* Add `user` propagation from `SagaManager` to `RequestExecutor`.
* Improve `SagaContext` behaviour, allowing to dynamically change field types and also delete them as in any `dict` instance.

0.2.0 (2021-11-08)
------------------

* Add compatibility to `minos-microservice-common>=0.2.0`.

0.3.0 (2021-11-15)
------------------

* Add `SagaService` class (containing the reply handling logic).
* Add `transactional_command` middleware (to extend the saga transactionality to remote steps).
* Add `TransactionCommitter` class with the responsibility to orchestrate the transaction commitment process of the saga.
* Add `minos-microservice-networks` and `minos-microservice-aggregate` as dependencies.
* Be compatible with `minos-microservice-common=^0.3.0`
* Integrate `minos.aggregate.transaction` into `Executor`.
* Replace `reply: CommandReply` by `response: SagaResponse`.
* Store `service_name: str` as part of `SagaStepExecution` metadata.
* Deprecate callback argument on `Saga.commit` method in favor of `Saga.local_step`.

0.3.1 (2021-11-17)
------------------

* Improve integration between `ConditionalSagaStep` and `TransactionCommitter`
* Add `SagaExecution.commit(...)` and `SagaExecution.reject(...)` methods and `autocommit: bool` and `autoreject: bool` into `SagaExecution.execute(...)` and `SagaExecution.rollback(...)` methods respectively.
* Fix concurrent blocks related with a limited amount of `minos.networks.DynamicHandler` instances on the `minos.networks.DynamicHandlerPool`.

0.3.2 (2021-11-23)
------------------

* Take advantage of concurrent remote broker calls to improve the `TransactionCommiter` performance.
* Expose `autocommit: bool = True` argument on `SagaManager.run(...)` method.
* Add support for `minos-microservice-networks=^0.3.0`.

0.3.3 (2021-11-26)
------------------

* Fix bug related with empty `ConditionalSagaStep.from_raw` and empty `else_then`.

0.3.4 (2021-11-26)
------------------

* Fix bug related with `TransactionCommitter` and `ConditionalSagaStepExecution`.
