"""minos.networks.handlers.dynamic.consumers module."""

from __future__ import (
    annotations,
)

from minos.common import (
    MinosConfig,
)

from ..abc import (
    Consumer,
)


class DynamicConsumer(Consumer):
    """Dynamic Consumer class."""

    TABLE_NAME = "dynamic_queue"

    @classmethod
    def _from_config(cls, *args, config: MinosConfig, **kwargs) -> DynamicConsumer:
        return cls(topics=set(), broker=config.broker, **config.broker.queue._asdict(), **kwargs)
