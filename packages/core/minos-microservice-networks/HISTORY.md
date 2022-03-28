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

0.0.18 (2021-10-04)
------------------

* Add `PeriodicTask`, `PeriodicTaskScheduler` and `PeriodicTaskSchedulerService`.
* Add `@enroute.periodic.event` decorator

0.0.19 (2021-11-03)
------------------

* Add `"user"` context variable to be accessible during `Request` handling (same as `Request.user`).
* Add support for `Request.user` propagation over `CommandBroker`.

0.1.0 (2021-11-08)
------------------

* Add `minos-microservice-common>=0.2.0` compatibility.

0.1.1 (2021-11-09)
------------------

* Add `REPLY_TOPIC_CONTEXT_VAR` and integrate with `DynamicHandlerPool`.
* Add support for `post_fn` callbacks following the same strategy as in `pre_fn` callbacks.

0.2.0 (2021-11-15)
------------------

* Remove dependency to `minos-microservice-aggregate` (now `minos.aggregate` package will require `minos.networks`).
* Add support for middleware functions.
* Add support variable number of services (previously only `CommandService` and `QueryService` were allowed).
* Migrate `Command`, `CommandReply`, `CommandStatus` and `Event` from `minos.common` to `minos.networks`.
* Add support for `minos-microservice-common=^0.3.0`

0.3.0 (2021-11-22)
------------------

* Improve `BrokerHandler` dispatching strategy to be more concurrent (using `asyncio.PriorityQueue` and multiple consumers).
* Add `send(...)` method to `DynamicBroker` in order to simplify the execution of request-response messages.
* Merge `minos.networks.brokers` and `minos.networks.handlers` modules into `minos.networks.brokers` (divided into `handlers` and `subscribers`).
* Merge `Command`, `CommandReply` and `Event` into `BrokerMessage`.
* Rename `CommandStatus` as `BrokerMessageStatus`.
* Merge `CommandBroker`, `CommandReplyBroker` and `EventBroker` into `BrokerPublisher`.
* Merge `CommandHandler`, `CommandReplyHandler` and `EventHandler` into `BrokerHandler`.
* Merge `CommandHandlerService`, `CommandReplyHandlerService` and `EventHandlerService` into `BrokerHandlerService`
* Rename `DynamicHandler` and `DynamicHandlerPool` as `DynamicBroker` and `DynamicBrokerPool`
* Rename `Consumer` and `ConsumerService` as `BrokerConsumer` and `BrokerConsumerService` respectively.
* Rename `Producer` and `ProducerService` as `BrokerProducer` and `BrokerProducerService` respectively.
* Rename `HandlerRequest`, `HandlerResponse` and `HandlerResponseException` as `BrokerRequest`, `BrokerResponse` and `BrokerResponseException` respectively.

0.3.1 (2021-11-30)
------------------

* Add `identifier: UUID` and `headers: dict[str, str]` attributes to `BrokerMessage`.
* Remove `saga: Optional[UUID]` and `service_name: str` attributes from `BrokerMessage`.
* Now `BrokerPublisher.send` returns the `BrokerMessage` identifier instead of the entry identifier on the `Producer`'s queue.
* Add `REQUEST_HEADERS_CONTEXT_VAR`.
* Rename `USER_CONTEXT_VAR` and `REPLY_TOPIC_CONTEXT_VAR` as `REQUEST_USER_CONTEXT_VAR` and `REQUEST_REPLY_TOPIC_CONTEXT_VAR` respectively.

0.3.2 (2021-12-27)
------------------

* Add `CheckDecorator` (accessible from `EnrouteDecorator.check(...): CheckDecorator` attribute) allowing to set check functions with the `(request: Request) -> bool` prototype to the service handling functions.
* Add support for more `Content-Type` values. Currently: `application/json`, `application/x-www-form-encoded`, `avro/binary`, `text/plain` and `application/octet-stream`.
* Remove url params and query params injection from the `RestRequest.content(..)` method.
* Add `Request.params(...)` method allowing to access to the request's params.
* Add `Request.has_content: bool` and `Request.has_params: bool` to check for the existence of `content` and `params` respectively.
* Add `InMemoryRequest` class that allows to create requests for testing or calling service handling functions directly.

0.4.0 (2022-01-27)
------------------

* Add `BrokerDispatcher` to break the direct relationship between `BrokerHandler` and `BrokerPublisher`.
* Add `content_type` argument to `RestResponse`'s constructor to be able to retrieve the result in a format other than `json`.
* Add versioning to `BrokerMessage` and implement the `BrokerMessageV1` and `BrokerMessageV1Payload` to be able to work with different microservice versions in the future.
* Refactor `BrokerPublisher.send` method to follow the `(message: BrokerMessage) -> None` prototype instead of a big list of arguments referred to the messages attributes.
* Refactor `brokers.publishers` module.
  * Add `BrokerPublisher` base class with a `send(message: BrokerMessage) -> Awaitable[None]` method.
  * Add `BrokerPublisherQueue` base class with an `enqueue(message: BrokerMessage) -> Awaitable[None]` and a `dequeue() -> Awaitable[BrokerMessage]` methods.
  * Add `KafkaBrokerPublisher` as the `kafka`'s implementation of the publisher.
  * Add `PostgreSqlBrokerPublisherQueue` as the `postgres` implementation of the publisher's message queue.
* Refactor `brokers.handlers`.
  * Add `BrokerSubscriber` base class with a `receive() -> Awaitable[BrokerMessage]` method.
  * Add `BrokerSubscriberQueue` base class with an `enqueue(message: BrokerMessage) -> Awaitable[None]` and a `dequeue() -> Awaitable[BrokerMessage]` methods.
  * Add `KafkaBrokerSubscriber` as the `kafka`'s implementation of the subscriber.
  * Add `PostgreSqlBrokerSubscriberQueue` as the `postgres` implementation of the subscriber's message queue.
* Refactor `DynamicBroker` and `DynamicBrokerPool` as `BrokerClient` and `BrokerClientPool`. The new `BrokerClient` has a `send(message: BrokerMessage) -> Awaitable[None]` method for sending messages and a `receive() -> Awaitable[BrokerMessage]` to receive them.
* Implement a builder pattern on `BrokerPublisher`
* Be compatible with `minos-microservice-common~=0.4.0`.

0.4.1 (2022-01-31)
------------------

* Update `README.md`.


0.5.0 (2022-02-03)
------------------

* Extract `kafka` related code to the `minos-broker-kafka` plugin.
* Extract `minos-discovery` related code to the `minos-discovery-minos` plugin.
* Minor changes.

0.5.1 (2022-02-03)
------------------

* Fix bug related with dependency specification.

0.5.2 (2022-02-08)
------------------

* Fix bug related with enroute decorator collisions in which the `MinosRedefinedEnrouteDecoratorException` was not raised. 
* Minor changes.

0.5.3 (2022-03-04)
------------------

* Improve error messages of  `BrokerDispatcher`, `RestHandler` and `PeriodicTask`.

0.6.0 (2022-03-28)
------------------

* Add `BrokerPort` class and deprecate `BrokerHandlerService`.
* Add `BrokerPublisherBuilder` to ease the building of `BrokerPublisher` instances.
* Add `FilteredBrokerSubscriber` implemented as a Chain-of-Responsibility Design Pattern to be able to filter `BrokerMessage` instances during subscription. 
* Add `BrokerSubscriberValidator` and `BrokerSubscriberDuplicateValidator` base classes and the `InMemoryBrokerSubscriberDuplicateValidator` and `PostgreSqlBrokerSubscriberDuplicateValidator` implementations.
* Rename `EnrouteAnalyzer` as `EnrouteCollector`.
* Rename `EnrouteBuilder` as `EnrouteFactory`.
* Add `HttpEnrouteDecorator`.
* Remove `KongDiscoveryClient` class (there are plans to re-include it as an external plugin in the future).
* Add `HttpConnector` as the base class that connects to the `http` server.
* Add `HttpAdapter` as the class that coordinates `HttpRouter` instances.
* Add `HttpPort` class and deprecate `RestService`.
* Add `HttpRequest`, `HttpResponse` and `HttpResponseException` as request/response wrappers of the `http` server. 
* Add `Router`, `HttpRouter`, `RestHttpRouter`, `BrokerRouter` and  `PeriodicRouter` as the classes that route requests to the corresponding services' handling functions 
* Add `PeriodicPort` class and deprecate `PeriodicTaskSchedulerService`.
* Add `CronTab` class to support `"@reboot"` pattern.
* Add `OpenAPIService` and `AsynAPIService` classes as the services that provide `openapi` and `asynciapi` specifications of the microservice.
* Add `SystemService` as the service that implements System Health checker.
* Replace `dependency-injector`'s injection classes by the ones provided by the `minos.common.injections` module.
* Be compatible with latest `minos.common.Config` API.