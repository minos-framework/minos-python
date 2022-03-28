from __future__ import (
    annotations,
)

import logging
from typing import (
    Optional,
)

from aio_pika import (
    Message,
    connect,
)

from minos.networks import (
    BrokerMessage,
    BrokerPublisher,
    BrokerPublisherBuilder,
)

from .common import (
    RabbitMQBrokerBuilderMixin,
)

logger = logging.getLogger(__name__)


class RabbitMQBrokerPublisher(BrokerPublisher):
    """RabbitMQ Broker Publisher class."""

    def __init__(
        self,
        *args,
        host: Optional[str] = None,
        port: Optional[int] = None,
        user: str = None,
        password: str = None,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)

        if host is None:
            host = "localhost"
        if port is None:
            port = 5672
        if user is None:
            user = "guest"
        if password is None:
            password = "guest"

        self.host = host
        self.port = port
        self.user = user
        self.password = password

        self.connection = None
        self.channel = None

    async def _setup(self) -> None:
        await super()._setup()
        self.connection = await connect(f"amqp://{self.user}:{self.password}@{self.host}:{self.port}/")
        self.channel = await self.connection.channel()

    async def _destroy(self) -> None:
        await self.channel.close()
        await self.connection.close()
        await super()._destroy()

    async def _send(self, message: BrokerMessage) -> None:
        await self.channel.default_exchange.publish(Message(message.avro_bytes), routing_key=message.topic)


class RabbitMQBrokerPublisherBuilder(BrokerPublisherBuilder[RabbitMQBrokerPublisher], RabbitMQBrokerBuilderMixin):
    """RabbitMQ Broker Publisher Builder class."""


RabbitMQBrokerPublisher.set_builder(RabbitMQBrokerPublisherBuilder)
