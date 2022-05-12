# History

## 0.5.0 (2022-02-03)

* Migrate `PostgreSqlQueuedKafkaBrokerPublisher` from `minos-microservice-networks`.
* Migrate `InMemoryQueuedKafkaBrokerPublisher` from `minos-microservice-networks`.
* Migrate `KafkaBrokerPublisher` from `minos-microservice-networks`.
* Migrate `KafkaBrokerSubscriber` from `minos-microservice-networks`.
* Migrate `KafkaBrokerSubscriberBuilder` from `minos-microservice-networks`.
* Migrate `PostgreSqlQueuedKafkaBrokerSubscriberBuilder` from `minos-microservice-networks`.
* Migrate `InMemoryQueuedKafkaBrokerSubscriberBuilder` from `minos-microservice-networks`.
* Minor changes.

## 0.5.1 (2022-02-03)

* Fix bug related with dependency specification.

## 0.6.0 (2022-03-28)

* Add `KafkaCircuitBreakerMixin` to integrate `minos.common.CircuitBreakerMixin` into the `KafkaBrokerPublisher` and `KafkaBrokerSubscriber` classes to be tolerant to connection failures to `kafka`.
* Add `KafkaBrokerPublisherBuilder` and `KafkaBrokerBuilderMixin` classes to ease the building process.

## 0.6.1 (2022-04-01)

* Improve `KafkaBrokerSubscriber`'s destroying process.

## 0.7.0 (2022-05-11)

* Remove `InMemoryQueuedKafkaBrokerPublisher`, `PostgreSqlQueuedKafkaBrokerPublisher`, `InMemoryQueuedKafkaBrokerSubscriberBuilder` and `PostgreSqlQueuedKafkaBrokerSubscriberBuilder` in favor of the use of `minos.networks.BrokerPublisherBuilder` and `minos.networks.BrokerSubscriberBuilder`.
* Unify documentation building pipeline across all `minos-python` packages.
* Fix documentation building warnings.
* Fix bug related with package building and additional files like `AUTHORS.md`, `HISTORY.md`, etc.