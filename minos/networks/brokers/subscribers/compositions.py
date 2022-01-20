from __future__ import annotations

from minos.common import MinosConfig

from .kafka import KafkaBrokerSubscriber
from .queued import (
    InMemoryBrokerSubscriberRepository,
    PostgreSqlBrokerSubscriberRepository,
    QueuedBrokerSubscriber,
)


class PostgreSqlQueuedKafkaBrokerSubscriber(QueuedBrokerSubscriber):
    """TODO"""

    @classmethod
    def _from_config(cls, config: MinosConfig, **kwargs) -> PostgreSqlQueuedKafkaBrokerSubscriber:
        impl = KafkaBrokerSubscriber.from_config(config, **kwargs)
        repository = PostgreSqlBrokerSubscriberRepository.from_config(config, **kwargs)
        return cls(impl, repository, **kwargs)


class InMemoryQueuedKafkaBrokerSubscriber(QueuedBrokerSubscriber):
    """TODO"""

    @classmethod
    def _from_config(cls, config: MinosConfig, **kwargs) -> InMemoryQueuedKafkaBrokerSubscriber:
        impl = KafkaBrokerSubscriber.from_config(config, **kwargs)
        repository = InMemoryBrokerSubscriberRepository.from_config(config, **kwargs)
        return cls(impl, repository, **kwargs)
