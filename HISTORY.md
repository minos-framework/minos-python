History
==========

0.0.3 (2021-05-24)
------------------

* renamed classes and removed Minos prefix
* Integration of Command and CommandReply with Handler

0.0.4 (2021-05-31)
------------------

* Documentation improvement
* BugFixes

0.0.5 (2021-05-31)
------------------

* BugFixes

0.0.6 (2021-06-02)
------------------

* Added Retry functionality at Broker
* Added Locked status at Broker
* Bugfix

0.0.7 (2021-07-06)
------------------

* Added basic approach for Circuit Braker ( Broker and Publisher)
* SQL queries refactor ( added support for psycopg method for query building )
* Migration of shapshot to Minos Common
* Updated Test
* BugFix
* Added clients for Minos Discovery Service
* Added support for Docker and Docker-Compose

0.0.8 (2021-07-12)
------------------

* Add new version support for minos-common
* BugFix

0.0.9 (2021-07-12)
------------------

* BugFix

0.0.10 (2021-07-26)
------------------

* Add `enroute` decorator.
* Add `ReplyHandlerPool`.
* Fix bug related with `DiscoveryConnector`.
* Add `WrappedRequest`.

0.0.11 (2021-07-27)
------------------

* Be compatible with `minos-microservice-common==0.1.7`.
* Fix bug related with `EventConsumerService`, `CommandConsumerService` and `CommandReplyConsumerService` and the `start` method.
* Fix bug related with `get_host_ip` function and some DNS systems.

0.0.12 (2021-08-03)
------------------

* Small Improvements
* Bugfixes

0.0.13 (2021-08-19)
------------------

* Update `DiscoveryConnector` to support auto discoverable endpoint.
* Increase the concurrency degree of `Handler.dispatch`.
* Rename `RestBuilder` as `RestHandler`.
* Refactor `HandlerEntry`

0.0.14 (2021-09-01)
------------------

* Unify consumer queues into a single one `consumer_queue`.
* Replace periodic checking (active waiting) by a `LISTEN/NOTIFY` approach (reactive) on consumer and producer queue.
* Use single reply topic based on microservice name to handle `CommandReply` messages of sagas paused on disk.
* Refactor `ReplyPool` and `DynamicReplyHandler` as `DynamicHandlerPool` and `DynamicHandler` and integrate them into the consumer queue.
* Improve `Producer` performance keeping kafka connection open between publishing calls.
* Implement direct message transferring between `Producer` and `Consumer` for messages send to the same microservice.

0.0.15 (2021-09-02)
------------------

* Add support for `__get_enroute__` method by `EnrouteAnalyzer`.

0.0.16 (2021-09-20)
------------------

* Add support for `Kong` discovery.
* Add support for `minos-microservice-common>=0.1.13`.
* Fix bug related with database queues and plain dates (without timezones).

0.0.17 (2021-09-27)
------------------

* Add support for multiple handling functions for events.
* Fix troubles related with dependency injections.
