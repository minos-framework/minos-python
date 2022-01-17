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
    PostgreSqlBrokerPublisherRepository,
    QueuedBrokerPublisher,
)


class PostgreSqlQueuedKafkaBrokerPublisher(QueuedBrokerPublisher):
    """TODO"""

    def __init__(self, impl: KafkaBrokerPublisher, repository: PostgreSqlBrokerPublisherRepository, *args, **kwargs):
        if not isinstance(impl, KafkaBrokerPublisher):
            raise ValueError("TODO")
        if not isinstance(repository, PostgreSqlBrokerPublisherRepository):
            raise ValueError("TODO")
        super().__init__(*args, **kwargs)

        self.impl = impl
        self.repository = repository

    @classmethod
    def _from_config(cls, config: MinosConfig, **kwargs) -> PostgreSqlQueuedKafkaBrokerPublisher:
        impl = KafkaBrokerPublisher.from_config(config, **kwargs)
        repository = PostgreSqlBrokerPublisherRepository.from_config(config, **kwargs)
        return cls(impl, repository, **kwargs)
