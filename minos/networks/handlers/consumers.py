"""minos.networks.abc.consumers module."""

from __future__ import (
    annotations,
)

import logging
from typing import (
    Any,
    NoReturn,
    Optional,
)

from aiokafka import (
    AIOKafkaConsumer,
)
from kafka.errors import (
    KafkaError,
)
from psycopg2.sql import (
    SQL,
    Identifier,
)

from minos.common import (
    BROKER,
    MinosConfig,
)

from ..decorators import (
    EnrouteBuilder,
)
from .abc import (
    HandlerSetup,
)

logger = logging.getLogger(__name__)


class Consumer(HandlerSetup):
    """
    Handler Server

    Generic insert for queue_* table. (Support Command, CommandReply and Event)

    """

    __slots__ = "_topics", "_broker", "_client"

    def __init__(
        self,
        topics: set[str] = None,
        broker: Optional[BROKER] = None,
        client: Optional[AIOKafkaConsumer] = None,
        group_id: Optional[str] = "default",
        **kwargs,
    ):
        super().__init__(**kwargs)
        if topics is None:
            topics = set()
        self._topics = set(topics)
        self._broker = broker
        self._client = client
        self._group_id = group_id

    @classmethod
    def _from_config(cls, config: MinosConfig, **kwargs) -> Consumer:
        topics = set()

        # events
        decorators = EnrouteBuilder(config.events.service, config).get_broker_event()
        topics |= {decorator.topic for decorator in decorators.keys()}

        # commands
        decorators = EnrouteBuilder(config.commands.service, config).get_broker_command_query()
        topics |= {decorator.topic for decorator in decorators.keys()}

        # queries
        decorators = EnrouteBuilder(config.queries.service, config).get_broker_command_query()
        topics |= {decorator.topic for decorator in decorators.keys()}

        # replies
        topics |= {f"{config.service.name}Reply"}

        return cls(
            topics=topics, broker=config.broker, group_id=config.service.name, **config.broker.queue._asdict(), **kwargs
        )

    async def _setup(self) -> None:
        await super()._setup()
        await self.client.start()

    async def _destroy(self) -> None:
        try:
            await self.client.stop()
        except KafkaError:  # pragma: no cover
            pass
        await super()._destroy()

    @property
    def topics(self) -> set[str]:
        """Topics getter.

        :return: A list of string values.
        """
        return self._topics

    async def add_topic(self, topic: str) -> None:
        """Add a topic to the consumer's subscribed topics.

        :param topic: Name of the topic to be added.
        :return: This method does not return anything.
        """
        self._topics.add(topic)
        self.client.subscribe(topics=list(self._topics))

    async def remove_topic(self, topic: str) -> None:
        """Remove a topic from the consumer's subscribed topics.

        :param topic: Name of the topic to be removed.
        :return: This method does not return anything.
        """
        self._topics.remove(topic)
        if len(self._topics):
            self.client.subscribe(topics=list(self._topics))
        else:
            self.client.unsubscribe()

    @property
    def client(self) -> AIOKafkaConsumer:
        if self._client is None:  # pragma: no cover
            self._client = AIOKafkaConsumer(
                *self._topics,
                bootstrap_servers=f"{self._broker.host}:{self._broker.port}",
                group_id=self._group_id,
                auto_offset_reset="earliest",
            )
        return self._client

    async def dispatch(self) -> NoReturn:
        """Perform a dispatching step.

        :return: This method does not return anything.
        """
        await self.handle_message(self.client)

    async def handle_message(self, consumer: Any) -> None:
        """Message consumer.

        It consumes the messages and sends them for processing.

        Args:
            consumer: Kafka Consumer instance (at the moment only Kafka consumer is supported).
        """

        async for message in consumer:
            await self.handle_single_message(message)

    async def handle_single_message(self, message):
        """Handle Kafka messages.

        Evaluate if the binary of message is an Event instance.
        Add Event instance to the event_queue table.

        Args:
            message: Kafka message.

        Raises:
            Exception: An error occurred inserting record.
        """
        logger.debug(f"Consuming message with {message.topic!s} topic...")

        return await self.enqueue(message.topic, message.partition, message.value)

    async def enqueue(self, topic: str, partition: int, binary: bytes) -> int:
        """Insert row into queue table.

        Retrieves number of affected rows and row ID.

        Args:
            topic: Kafka topic. Example: "TicketAdded"
            partition: Kafka partition number.
            binary: Event Model in bytes.

        Returns:
            Queue ID.

            Example: 12

        Raises:
            Exception: An error occurred inserting record.
        """
        row = await self.submit_query_and_fetchone(_INSERT_QUERY, (topic, partition, binary))
        await self.submit_query(_NOTIFY_QUERY.format(Identifier(topic)))

        return row[0]


_INSERT_QUERY = SQL(
    "INSERT INTO consumer_queue (topic, partition_id, binary_data, creation_date) "
    "VALUES (%s, %s, %s, NOW()) "
    "RETURNING id"
)

_NOTIFY_QUERY = SQL("NOTIFY {}")
