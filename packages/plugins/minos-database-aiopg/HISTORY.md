# History

## 0.7.0 (2022-05-11)

* Add `AiopgDatabaseClient` as the `minos.common.DatabaseClient` implementation for `postgres`.
* Add `AiopgDatabaseOperation` as the `minos.common.DatabaseOperation` implementation for `postgres`.
* Add `AiopgLockDatabaseOperationFactory` as the `minos.common.LockDatabaseOperationFactory` implementation for `postgres`.
* Add `AiopgManagementDatabaseOperationFactory` as the `minos.common.ManagementDatabaseOperationFactory` implementation for `postgres`.
* Add `AiopgBrokerPublisherQueueDatabaseOperationFactory` as the `minos.networks.BrokerPublisherQueueDatabaseOperationFactory` implementation for `postgres`.
* Add `AiopgBrokerQueueDatabaseOperationFactory` as the `minos.networks.BrokerQueueDatabaseOperationFactory` implementation for `postgres`.
* Add `AiopgBrokerSubscriberDuplicateValidatorDatabaseOperationFactory` as the `minos.networks.BrokerSubscriberDuplicateValidatorDatabaseOperationFactory` implementation for `postgres`.
* Add `AiopgBrokerSubscriberQueueDatabaseOperationFactory` as the `minos.networks.BrokerSubscriberQueueDatabaseOperationFactory` implementation for `postgres`.
* Add `AiopgEventDatabaseOperationFactory` as the `minos.aggregate.EventDatabaseOperationFactory` implementation for `postgres`.
* Add `AiopgSnapshotDatabaseOperationFactory` as the `minos.aggregate.SnapshotDatabaseOperationFactory` implementation for `postgres`.
* Add `AiopgSnapshotQueryDatabaseOperationBuilder` to ease the complex snapshot's query building for `postgres`.
* Add `AiopgTransactionDatabaseOperationFactory` as the `minos.aggregate.TransactionDatabaseOperationFactory` implementation for `postgres`.
