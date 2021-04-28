# Copyright (C) 2020 Clariteia SL
#
# This file is part of minos framework.
#
# Minos framework can not be copied and/or distributed without the express
# permission of Clariteia SL.

import asyncio
import functools
import typing as t
import datetime

from aiokafka import AIOKafkaConsumer
from aiomisc import Service
import aiopg
from minos.common.configuration.config import MinosConfig
from minos.common.importlib import import_module
from minos.common.logs import log

from minos.networks.exceptions import MinosNetworkException
from minos.common.broker import Event

class MinosEventServer(Service):
    """
    Event Manager

    Consumer for the Broker ( at the moment only Kafka is supported )

    """
    __slots__ = "_tasks", "_db_dsn", "_handlers", "_topics", "_kafka_conn_data", "_broker_group_name"

    def __init__(self, *, conf: MinosConfig, **kwargs: t.Any):
        self._tasks = set()  # type: t.Set[asyncio.Task]
        self._db_dsn = f"dbname={conf.events.queue.database} user={conf.events.queue.user} " \
                       f"password={conf.events.queue.password} host={conf.events.queue.host}"
        self._handler = {item.name: {'controller': item.controller, 'action': item.action}
                         for item in conf.events.items}
        self._topics = list(self._handler.keys())
        self._kafka_conn_data = f"{conf.events.broker.host}:{conf.events.broker.port}"
        self._broker_group_name = f"event_{conf.service.name}"
        super().__init__(**kwargs)

    def create_task(self, coro: t.Awaitable[t.Any]):
        task = self.loop.create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.remove)

    async def event_queue_add(self, topic: str, partition: int, binary: bytes):
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
                        "INSERT INTO event_queue (topic, partition_id, binary_data, creation_date) VALUES (%s, %s, %s, %s) RETURNING id;",
                        (topic, partition, binary, datetime.datetime.now(),),
                    )

                    queue_id = await cur.fetchone()
                    affected_rows = cur.rowcount

        return affected_rows, queue_id[0]

    async def handle_message(self, consumer: t.Any):
        """Handle Kafka messages.

        For each message evaluate if the binary is an Event instance.
        Add Event instance to the event_queue table.

        Args:
            consumer: Kafka Consumer instance (at the moment only Kafka consumer is supported).

        Raises:
            Exception: An error occurred inserting record.
        """

        async for msg in consumer:
            # the handler receive a message and store in the queue database
            topic = msg.topic
            partition = msg.partition
            event_binary = msg.value
            # check if the event binary string is well formatted
            try:
                event_instance = Event.from_avro_bytes(event_binary)
                await self.event_queue_add(msg.topic, msg.partition, event_binary)
            except:
                pass

    async def start(self) -> t.Any:
        self.start_event.set()
        log.debug("Event Consumer Manager: Started")
        # start the Service Event Consumer for Kafka
        consumer = AIOKafkaConsumer(loop=self.loop,
                                    group_id=self._broker_group_name,
                                    auto_offset_reset="latest",
                                    bootstrap_servers=self._kafka_conn_data,
                                    )

        await consumer.start()
        consumer.subscribe(self._topics)

        self.create_task(self.handle_message(consumer))

    def _get_event_handler(self, topic: str) -> t.Callable:
        for event in self._handlers:
            if event.name == topic:
                # the topic exist, get the controller and the action
                controller = event.controller
                action = event.action
                object_class = import_module(controller)
                instance_class = object_class()
                return functools.partial(instance_class.action)
        raise MinosNetworkException(f"topic {topic} have no controller/action configured, "
                                    f"please review th configuration file")


async def event_handler_table_creation(conf: MinosConfig):
    db_dsn = f"dbname={conf.events.queue.database} user={conf.events.queue.user} " \
                   f"password={conf.events.queue.password} host={conf.events.queue.host}"
    async with aiopg.create_pool(db_dsn) as pool:
        async with pool.acquire() as connect:
            async with connect.cursor() as cur:
                await cur.execute(
                    'CREATE TABLE IF NOT EXISTS "event_queue" ("id" SERIAL NOT NULL PRIMARY KEY, '
                    '"topic" VARCHAR(255) NOT NULL, "partition_id" INTEGER , "binary_data" BYTEA NOT NULL, "creation_date" TIMESTAMP NOT NULL);'
                )


class EventHandlerDatabaseInitializer(Service):
    async def start(self):
        # Send signal to entrypoint for continue running
        self.start_event.set()

        await event_handler_table_creation(conf=self.config)

        await self.stop(self)

