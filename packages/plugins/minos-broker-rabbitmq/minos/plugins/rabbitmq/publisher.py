from __future__ import (
    annotations,
)

import logging

from aio_pika import connect, Message
from aio_pika.abc import AbstractConnection
from cached_property import cached_property

from minos.common import (
    MinosConfig,
)
from minos.networks import (
    BrokerMessage,
    BrokerPublisher,
    InMemoryBrokerPublisherQueue,
    PostgreSqlBrokerPublisherQueue,
    QueuedBrokerPublisher,
)

logger = logging.getLogger(__name__)


class PostgreSqlQueuedRabbitMQBrokerPublisher(QueuedBrokerPublisher):
    """PostgreSql Queued RabbitMQ Broker Publisher class."""

    @classmethod
    def _from_config(cls, config: MinosConfig, **kwargs) -> PostgreSqlQueuedRabbitMQBrokerPublisher:
        impl = RabbitMQBrokerPublisher.from_config(config, **kwargs)
        queue = PostgreSqlBrokerPublisherQueue.from_config(config, **kwargs)
        return cls(impl, queue, **kwargs)


class InMemoryQueuedRabbitMQBrokerPublisher(QueuedBrokerPublisher):
    """In Memory Queued RabbitMQ Broker Publisher class."""

    @classmethod
    def _from_config(cls, config: MinosConfig, **kwargs) -> InMemoryQueuedRabbitMQBrokerPublisher:
        impl = RabbitMQBrokerPublisher.from_config(config, **kwargs)
        queue = InMemoryBrokerPublisherQueue.from_config(config, **kwargs)
        return cls(impl, queue, **kwargs)


class RabbitMQBrokerPublisher(BrokerPublisher):
    """RabbitMQ Broker Publisher class."""

    def __init__(self, *args, broker_host: str, broker_port: int, **kwargs):
        super().__init__(*args, **kwargs)
        self.broker_host = broker_host
        self.broker_port = broker_port

    @classmethod
    def _from_config(cls, config: MinosConfig, **kwargs) -> RabbitMQBrokerPublisher:
        kwargs["broker_host"] = config.broker.host
        kwargs["broker_port"] = config.broker.port
        # noinspection PyProtectedMember
        return cls(**config.broker.queue._asdict(), **kwargs)

    async def _setup(self) -> None:
        await super()._setup()
        self.connection = await connect(f"amqp://guest:guest@{self.broker_host}/")

    async def _destroy(self) -> None:
        await self.connection.close()

    async def _send(self, message: BrokerMessage) -> None:
        async with self.connection:
            channel = await self.connection.channel()
            queue = await channel.declare_queue(message.topic)
            await channel.default_exchange.publish(Message(message.avro_bytes), routing_key=queue.name)
