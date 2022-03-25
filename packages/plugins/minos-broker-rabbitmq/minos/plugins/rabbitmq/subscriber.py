from __future__ import (
    annotations,
)

import logging
from collections.abc import (
    Iterable,
)
from typing import (
    Optional,
)

from aio_pika import (
    connect,
)
from aio_pika.exceptions import (
    QueueEmpty,
)

from minos.common import (
    Config,
    MinosConfig,
)
from minos.networks import (
    BrokerMessage,
    BrokerSubscriber,
    BrokerSubscriberBuilder,
    InMemoryBrokerSubscriberQueueBuilder,
    PostgreSqlBrokerSubscriberQueueBuilder,
    QueuedBrokerSubscriberBuilder,
)
from minos.plugins.rabbitmq import (
    RabbitMQBrokerBuilderMixin,
)

logger = logging.getLogger(__name__)


class RabbitMQBrokerSubscriber(BrokerSubscriber):
    """RabbitMQ Broker Subscriber class."""

    def __init__(
        self,
        topics: Iterable[str],
        host: str,
        port: int,
        group_id: Optional[str] = None,
        remove_topics_on_destroy: bool = False,
        **kwargs,
    ):
        super().__init__(topics, **kwargs)
        self.host = host
        self.port = port
        self.group_id = group_id

        self.remove_topics_on_destroy = remove_topics_on_destroy

    async def _setup(self) -> None:
        await super()._setup()
        self.connection = await connect(f"amqp://guest:guest@{self.host}:{self.port}/")

    async def _destroy(self) -> None:
        await self.connection.close()

    async def _receive(self) -> BrokerMessage:
        for topic in self.topics:
            async with self.connection:
                channel = await self.connection.channel()
                queue = await channel.declare_queue(topic)
                try:
                    message = await queue.get()
                    return BrokerMessage.from_avro_bytes(message.body)
                except QueueEmpty:
                    pass


class RabbitMQBrokerSubscriberBuilder(BrokerSubscriberBuilder[RabbitMQBrokerSubscriber], RabbitMQBrokerBuilderMixin):
    """RabbitMQ Broker Subscriber Builder class."""


RabbitMQBrokerSubscriber.set_builder(RabbitMQBrokerSubscriberBuilder)
