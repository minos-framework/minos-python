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
import typing as t
from abc import (
    abstractmethod,
)
from typing import (
    Any,
    AsyncIterator,
    NamedTuple,
    Optional,
)

import aiopg
from aiokafka import (
    AIOKafkaConsumer,
)
from psycopg2.extensions import (
    AsIs,
)

from minos.common import (
    MinosConfig,
)

from .abc import (
    MinosHandlerSetup,
)


class MinosHandlerServer(MinosHandlerSetup):
    """
    Handler Server

    Generic insert for queue_* table. (Support Command, CommandReply and Event)

    """

    __slots__ = "_tasks", "_db_dsn", "_handlers", "_topics", "_broker_group_name"

    def __init__(self, *, table_name: str, config: NamedTuple, **kwargs: Any):
        super().__init__(table_name=table_name, **kwargs, **config.queue._asdict())
        self._tasks = set()  # type: t.Set[asyncio.Task]
        self._db_dsn = (
            f"dbname={config.queue.database} user={config.queue.user} "
            f"password={config.queue.password} host={config.queue.host}"
        )
        self._handler = {item.name: {"controller": item.controller, "action": item.action} for item in config.items}
        self._topics = list(self._handler.keys())
        self._table_name = table_name

    @classmethod
    def from_config(cls, *args, config: MinosConfig = None, **kwargs) -> Optional[MinosHandlerServer]:
        """Build a new repository from config.
        :param args: Additional positional arguments.
        :param config: Config instance. If `None` is provided, default config is chosen.
        :param kwargs: Additional named arguments.
        :return: A `MinosRepository` instance.
        """
        if config is None:
            config = MinosConfig.get_default()
        if config is None:
            return None
        # noinspection PyProtectedMember
        return cls(*args, config=config, **kwargs)

    async def queue_add(self, topic: str, partition: int, binary: bytes):
        """Insert row to event_queue table.

        Retrieves number of affected rows and row ID.

        Args:
            topic: Kafka topic. Example: "TicketAdded"
            partition: Kafka partition number.
            binary: Event Model in bytes.

        Returns:
            Affected rows and queue ID.

            Example: 1, 12

        Raises:
            Exception: An error occurred inserting record.
        """

        async with aiopg.create_pool(self._db_dsn) as pool:
            async with pool.acquire() as connect:
                async with connect.cursor() as cur:
                    await cur.execute(
                        _INSERT_QUERY, (AsIs(self._table_name), topic, partition, binary, datetime.datetime.now(),),
                    )

                    queue_id = await cur.fetchone()
                    affected_rows = cur.rowcount

        return affected_rows, queue_id[0]

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
        affected_rows, id = await self.queue_add(msg.topic, msg.partition, msg.value)
        return affected_rows, id

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
    async def kafka_consumer(topics: list, group_name: str, conn: str):
        # start the Service Event Consumer for Kafka
        consumer = AIOKafkaConsumer(group_id=group_name, auto_offset_reset="latest", bootstrap_servers=conn,)

        await consumer.start()
        consumer.subscribe(topics)

        return consumer


_INSERT_QUERY = """
INSERT INTO %s (topic, partition_id, binary_data, creation_date)
VALUES (%s, %s, %s, %s)
RETURNING id;
""".strip()
