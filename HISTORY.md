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
