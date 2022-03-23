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

from minos.common import (
    MinosConfig,
    Config,
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
        self.connection = await connect(f"amqp://guest:guest@{self.broker_host}/")

    async def _destroy(self) -> None:
        await self.connection.close()

    async def _receive(self) -> BrokerMessage:
        for topic in self.topics:
            async with self.connection:
                channel = await self.connection.channel()
                queue = await channel.declare_queue(topic)
                message = await queue.get(fail=False)
                return BrokerMessage.from_avro_bytes(message.body if message.body else None)


class RabbitMQBrokerSubscriberBuilder(BrokerSubscriberBuilder):
    """RabbitMQ Broker Subscriber Builder class."""

    def with_config(self, config: Config) -> BrokerSubscriberBuilder:
        """Set config.

        :param config: The config to be set.
        :return: This method return the builder instance.
        """
        broker_config = config.get_interface_by_name("broker")
        common_config = broker_config["common"]

        self.kwargs |= {
            "group_id": config.get_name(),
            "broker_host": common_config["host"],
            "broker_port": common_config["port"],
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
