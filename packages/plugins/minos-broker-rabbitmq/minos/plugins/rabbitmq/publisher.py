from __future__ import (
    annotations,
)

import logging

from aio_pika import connect, Message

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

    async def _destroy(self) -> None:
        pass

    async def _send(self, message: BrokerMessage) -> None:
        connection = await connect(f"amqp://guest:guest@{self.broker_host}/")
        async with connection:
            channel = await connection.channel()
            queue = await channel.declare_queue(message.topic)
            await channel.default_exchange.publish(
                Message(message.avro_bytes),
                routing_key=queue.name
            )
