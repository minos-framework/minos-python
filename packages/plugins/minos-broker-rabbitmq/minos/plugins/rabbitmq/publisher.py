from __future__ import (
    annotations,
)

import logging

from aio_pika import (
    Message,
    connect,
)

from minos.common import (
    MinosConfig,
)
from minos.networks import (
    BrokerMessage,
    BrokerPublisher,
    InMemoryBrokerPublisherQueue,
    PostgreSqlBrokerPublisherQueue,
    QueuedBrokerPublisher, BrokerPublisherBuilder,
)
from minos.plugins.rabbitmq.common import RabbitMQBrokerBuilderMixin

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

    def __init__(self, *args, host: str, port: int, **kwargs):
        super().__init__(*args, **kwargs)
        self.broker_host = host
        self.broker_port = port

    # @classmethod
    # def _from_config(cls, config: MinosConfig, **kwargs) -> RabbitMQBrokerPublisher:
    #     broker_config = config.get_interface_by_name("broker")
    #     common_config = broker_config["common"]
    #
    #     kwargs["broker_host"] = common_config["host"]
    #     kwargs["broker_port"] = common_config["port"]
    #     return cls(**kwargs)

    async def _setup(self) -> None:
        await super()._setup()
        self.connection = await connect(f"amqp://guest:guest@{self.broker_host}:{self.broker_port}/")

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
