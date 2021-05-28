# Copyright (C) 2020 Clariteia SL
#
# This file is part of minos framework.
#
# Minos framework can not be copied and/or distributed without the express
# permission of Clariteia SL.
from __future__ import (
    annotations,
)

import datetime
from abc import (
    abstractmethod,
)
from typing import (
    Any,
    NoReturn,
    Optional,
)

from aiokafka import (
    AIOKafkaConsumer,
)
from psycopg2.extensions import (
    AsIs,
)

from minos.common import (
    MinosConfig,
)

from .setups import (
    HandlerSetup,
)


class Consumer(HandlerSetup):
    """
    Handler Server

    Generic insert for queue_* table. (Support Command, CommandReply and Event)

    """

    __slots__ = "_topics", "_kafka_connection_data", "__consumer"

    def __init__(
        self,
        *,
        topics: list[str],
        kafka_connection_data: Optional[str] = None,
        consumer: Optional[Any] = None,
        **kwargs
    ):
        super().__init__(**kwargs)
        self._topics = topics
        self._kafka_connection_data = kafka_connection_data
        self.__consumer = consumer

    @classmethod
    def _from_config(cls, *args, config: MinosConfig, **kwargs) -> Consumer:
        return cls(*args, config=config, **kwargs)

    async def _setup(self) -> NoReturn:
        await super()._setup()
        await self._consumer.start()

    @property
    def _consumer(self) -> AIOKafkaConsumer:
        if self.__consumer is None:  # pragma: no cover
            self.__consumer = AIOKafkaConsumer(*self._topics, bootstrap_servers=self._kafka_connection_data)
        return self.__consumer

    async def _destroy(self) -> NoReturn:
        await self._consumer.stop()
        await super()._destroy()

    async def dispatch(self) -> NoReturn:
        """Perform a dispatching step.

        :return: This method does not return anything.
        """
        await self.handle_message(self._consumer)

    async def handle_message(self, consumer: Any) -> NoReturn:
        """Message consumer.

        It consumes the messages and sends them for processing.

        Args:
            consumer: Kafka Consumer instance (at the moment only Kafka consumer is supported).
        """

        async for msg in consumer:
            await self.handle_single_message(msg)

    async def handle_single_message(self, msg):
        """Handle Kafka messages.

        Evaluate if the binary of message is an Event instance.
        Add Event instance to the event_queue table.

        Args:
            msg: Kafka message.

        Raises:
            Exception: An error occurred inserting record.
        """
        # the handler receive a message and store in the queue database
        # check if the event binary string is well formatted
        if not self._is_valid_instance(msg.value):
            return

        return await self.queue_add(msg.topic, msg.partition, msg.value)

    @abstractmethod
    def _is_valid_instance(self, value: bytes):  # pragma: no cover
        raise Exception("Method not implemented")

    async def queue_add(self, topic: str, partition: int, binary: bytes) -> int:
        """Insert row to event_queue table.

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
        queue_id = await self.submit_query_and_fetchone(
            _INSERT_QUERY, (AsIs(self.TABLE_NAME), topic, partition, binary, datetime.datetime.now()),
        )

        return queue_id[0]


_INSERT_QUERY = """
INSERT INTO %s (topic, partition_id, binary_data, creation_date)
VALUES (%s, %s, %s, %s)
RETURNING id;
""".strip()
