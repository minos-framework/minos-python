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

    def add_topic(self, topic: str) -> None:
        """TODO

        :param topic: TODO
        :return: TODO
        """
        self._topics.add(topic)
        self._consumer.subscribe(topics=list(self._topics))

    def remove_topic(self, topic: str) -> None:
        """TODO

        :param topic: TODO
        :return: TODO
        """
        self._topics.remove(topic)
        if len(self._topics):
            self._consumer.subscribe(topics=list(self._topics))
        else:
            self._consumer.unsubscribe()
