from __future__ import (
    annotations,
)

import logging

from aio_pika import (
    Message,
    connect,
)

from minos.networks import (
    BrokerMessage,
    BrokerPublisher,
    BrokerPublisherBuilder,
)
from minos.plugins.rabbitmq.common import (
    RabbitMQBrokerBuilderMixin,
)

logger = logging.getLogger(__name__)


class RabbitMQBrokerPublisher(BrokerPublisher):
    """RabbitMQ Broker Publisher class."""

    def __init__(self, *args, host: str, port: int, **kwargs):
        super().__init__(*args, **kwargs)
        self.host = host
        self.port = port

    async def _setup(self) -> None:
        await super()._setup()
        self.connection = await connect(f"amqp://guest:guest@{self.host}:{self.port}/")

    async def _destroy(self) -> None:
        await self.connection.close()

    async def _send(self, message: BrokerMessage) -> None:
        async with self.connection:
            channel = await self.connection.channel()
            queue = await channel.declare_queue(message.topic)
            await channel.default_exchange.publish(Message(message.avro_bytes), routing_key=queue.name)


class RabbitMQBrokerPublisherBuilder(BrokerPublisherBuilder[RabbitMQBrokerPublisher], RabbitMQBrokerBuilderMixin):
    """RabbitMQ Broker Publisher Builder class."""


RabbitMQBrokerPublisher.set_builder(RabbitMQBrokerPublisherBuilder)
