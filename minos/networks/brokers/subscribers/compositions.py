from __future__ import (
    annotations,
)

from minos.common import (
    MinosConfig,
)

from .kafka import (
    KafkaBrokerSubscriber,
)
from .queued import (
    InMemoryBrokerSubscriberQueue,
    PostgreSqlBrokerSubscriberQueue,
    QueuedBrokerSubscriber,
)


class PostgreSqlQueuedKafkaBrokerSubscriber(QueuedBrokerSubscriber):
    """PostgreSql Queued Kafka Broker Subscriber class."""

    @classmethod
    def _from_config(cls, config: MinosConfig, **kwargs) -> PostgreSqlQueuedKafkaBrokerSubscriber:
        impl = KafkaBrokerSubscriber.from_config(config, **kwargs)
        queue = PostgreSqlBrokerSubscriberQueue.from_config(config, **kwargs)
        return cls(impl=impl, queue=queue, **kwargs)


class InMemoryQueuedKafkaBrokerSubscriber(QueuedBrokerSubscriber):
    """In Memory Queued Kafka Broker Subscriber class."""

    @classmethod
    def _from_config(cls, config: MinosConfig, **kwargs) -> InMemoryQueuedKafkaBrokerSubscriber:
        impl = KafkaBrokerSubscriber.from_config(config, **kwargs)
        queue = InMemoryBrokerSubscriberQueue.from_config(config, **kwargs)
        return cls(impl=impl, queue=queue, **kwargs)
