from __future__ import (
    annotations,
)

import asyncio
import logging
from asyncio import (
    TimeoutError,
    wait_for,
)
from collections.abc import (
    Iterable,
)
from contextlib import (
    suppress,
)
from typing import (
    Optional,
)

from aio_pika import connect
from aio_pika.abc import AbstractConnection, AbstractIncomingMessage
from aiokafka import (
    AIOKafkaConsumer,
)
from cached_property import (
    cached_property,
)
from kafka import (
    KafkaAdminClient,
)
from kafka.admin import (
    NewTopic,
)
from kafka.errors import (
    TopicAlreadyExistsError,
)

from minos.common import (
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

logger = logging.getLogger(__name__)


class RabbitMQBrokerSubscriber(BrokerSubscriber):
    """RabbitMQ Broker Subscriber class."""

    def __init__(
        self,
        topics: Iterable[str],
        broker_host: str,
        broker_port: int,
        group_id: Optional[str] = None,
        remove_topics_on_destroy: bool = False,
        **kwargs,
    ):
        super().__init__(topics, **kwargs)
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.group_id = group_id

        self.remove_topics_on_destroy = remove_topics_on_destroy

    @classmethod
    def _from_config(cls, config: MinosConfig, **kwargs) -> RabbitMQBrokerSubscriber:
        # noinspection PyTypeChecker
        return RabbitMQBrokerSubscriberBuilder.new().with_config(config).with_kwargs(kwargs).build()

    async def _setup(self) -> None:
        await super()._setup()

    async def _destroy(self) -> None:
        pass

    async def _receive(self) -> BrokerMessage:
        for topic in self.topics:
            connection = await connect(f"amqp://guest:guest@{self.broker_host}/")
            async with connection:
                channel = await connection.channel()
                queue = await channel.declare_queue(topic)
                message = await queue.get()
                return BrokerMessage.from_avro_bytes(message.body)


class RabbitMQBrokerSubscriberBuilder(BrokerSubscriberBuilder):
    """RabbitMQ Broker Subscriber Builder class."""

    def with_config(self, config: MinosConfig) -> BrokerSubscriberBuilder:
        """Set config.

        :param config: The config to be set.
        :return: This method return the builder instance.
        """
        self.kwargs |= {
            "group_id": config.service.name,
            "broker_host": config.broker.host,
            "broker_port": config.broker.port,
        }
        return self

    def build(self) -> BrokerSubscriber:
        """Build the instance.

        :return: A ``RabbitMQBrokerSubscriber`` instance.
        """
        return RabbitMQBrokerSubscriber(**self.kwargs)


class PostgreSqlQueuedRabbitMQBrokerSubscriberBuilder(QueuedBrokerSubscriberBuilder):
    """PostgreSql Queued RabbitMQ Broker Subscriber Builder class."""

    def __init__(self, *args, **kwargs):
        super().__init__(
            *args,
            impl_builder=RabbitMQBrokerSubscriberBuilder.new(),
            queue_builder=PostgreSqlBrokerSubscriberQueueBuilder.new(),
            **kwargs,
        )


class InMemoryQueuedRabbitMQBrokerSubscriberBuilder(QueuedBrokerSubscriberBuilder):
    """In Memory Queued RabbitMQ Broker Subscriber Builder class."""

    def __init__(self, *args, **kwargs):
        super().__init__(
            *args,
            impl_builder=RabbitMQBrokerSubscriberBuilder.new(),
            queue_builder=InMemoryBrokerSubscriberQueueBuilder.new(),
            **kwargs,
        )
