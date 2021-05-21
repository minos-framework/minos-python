# Copyright (C) 2020 Clariteia SL
#
# This file is part of minos framework.
#
# Minos framework can not be copied and/or distributed without the express
# permission of Clariteia SL.
from __future__ import (
    annotations,
)

import asyncio
import datetime
from abc import (
    abstractmethod,
)
from typing import (
    Any,
    AsyncIterator,
    NoReturn,
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

    __slots__ = "_tasks", "_handler", "_topics", "_table_name", "_broker_group_name", "_kafka_conn_data", "_consumer"

    def __init__(self, *, table_name: str, config, **kwargs: Any):
        super().__init__(table_name=table_name, **kwargs, **config.queue._asdict())
        self._tasks = set()  # type: set[asyncio.Task]
        self._handler = {item.name: {"controller": item.controller, "action": item.action} for item in config.items}
        self._topics = list(self._handler.keys())
        self._table_name = table_name
        self._broker_group_name = None
        self._kafka_conn_data = None
        self._consumer = None

    @classmethod
    def _from_config(cls, *args, config: MinosConfig, **kwargs) -> Consumer:
        return cls(*args, config=config, **kwargs)

    async def _setup(self) -> NoReturn:
        self._consumer = await self._build_kafka_consumer(self._topics, self._broker_group_name, self._kafka_conn_data)
        await super()._setup()

    async def _destroy(self) -> NoReturn:
        await self._consumer.stop()
        await super()._destroy()

    async def dispatch(self) -> NoReturn:
        """Perform a dispatching step.

        :return: This method does not return anything.
        """
        await self.handle_message(self._consumer)

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
            _INSERT_QUERY, (AsIs(self._table_name), topic, partition, binary, datetime.datetime.now(),),
        )

        return queue_id[0]

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

    async def handle_message(self, consumer: AsyncIterator):
        """Message consumer.

        It consumes the messages and sends them for processing.

        Args:
            consumer: Kafka Consumer instance (at the moment only Kafka consumer is supported).
        """

        async for msg in consumer:
            await self.handle_single_message(msg)

    @staticmethod
    async def _build_kafka_consumer(topics: list, group_name: str, conn: str) -> AIOKafkaConsumer:
        # start the Service Event Consumer for Kafka
        consumer = AIOKafkaConsumer(group_id=group_name, auto_offset_reset="latest", bootstrap_servers=conn)

        await consumer.start()
        consumer.subscribe(topics)

        return consumer


_INSERT_QUERY = """
INSERT INTO %s (topic, partition_id, binary_data, creation_date)
VALUES (%s, %s, %s, %s)
RETURNING id;
""".strip()
