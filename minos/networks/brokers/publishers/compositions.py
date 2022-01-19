from __future__ import (
    annotations,
)

from minos.common import (
    MinosConfig,
)

from .kafka import (
    KafkaBrokerPublisher,
)
from .queued import (
    InMemoryBrokerPublisherRepository,
    PostgreSqlBrokerPublisherRepository,
    QueuedBrokerPublisher,
)


class PostgreSqlQueuedKafkaBrokerPublisher(QueuedBrokerPublisher):
    """TODO"""

    @classmethod
    def _from_config(cls, config: MinosConfig, **kwargs) -> PostgreSqlQueuedKafkaBrokerPublisher:
        impl = KafkaBrokerPublisher.from_config(config, **kwargs)
        repository = PostgreSqlBrokerPublisherRepository.from_config(config, **kwargs)
        return cls(impl, repository, **kwargs)


class InMemoryQueuedKafkaBrokerPublisher(QueuedBrokerPublisher):
    """TODO"""

    @classmethod
    def _from_config(cls, config: MinosConfig, **kwargs) -> InMemoryQueuedKafkaBrokerPublisher:
        impl = KafkaBrokerPublisher.from_config(config, **kwargs)
        repository = InMemoryBrokerPublisherRepository.from_config(config, **kwargs)
        return cls(impl, repository, **kwargs)
