from minos.common import (
    MinosConfig,
)

from ..subscribers import (
    InMemoryQueuedKafkaBrokerSubscriber,
    KafkaBrokerSubscriber,
    PostgreSqlQueuedKafkaBrokerSubscriber,
)
from .impl import (
    BrokerHandler,
)


class InMemoryQueuedKafkaBrokerHandler(BrokerHandler):
    """TODO"""

    @classmethod
    def _from_config(cls, config: MinosConfig, **kwargs) -> BrokerHandler:
        dispatcher = cls._get_dispatcher(config, **kwargs)
        topics = set(dispatcher.actions.keys())
        subscriber = InMemoryQueuedKafkaBrokerSubscriber.from_config(config, topics=topics)
        return cls(dispatcher, subscriber, **kwargs)


class KafkaBrokerHandler(BrokerHandler):
    """TODO"""

    @classmethod
    def _from_config(cls, config: MinosConfig, **kwargs) -> BrokerHandler:
        dispatcher = cls._get_dispatcher(config, **kwargs)
        topics = set(dispatcher.actions.keys())
        subscriber = KafkaBrokerSubscriber.from_config(config, topics=topics)
        return cls(dispatcher, subscriber, **kwargs)


class PostgreSqlQueuedKafkaBrokerHandler(BrokerHandler):
    """TODO"""

    @classmethod
    def _from_config(cls, config: MinosConfig, **kwargs) -> BrokerHandler:
        dispatcher = cls._get_dispatcher(config, **kwargs)
        topics = set(dispatcher.actions.keys())
        subscriber = PostgreSqlQueuedKafkaBrokerSubscriber.from_config(config, topics=topics)
        return cls(dispatcher, subscriber, **kwargs)
