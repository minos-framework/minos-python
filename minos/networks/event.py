# Copyright (C) 2020 Clariteia SL
#
# This file is part of minos framework.
#
# Minos framework can not be copied and/or distributed without the express
# permission of Clariteia SL.

import asyncio
import datetime
import functools
import inspect
import typing as t

import aiopg
from aiokafka import (
    AIOKafkaConsumer,
)
from aiomisc import (
    Service,
)
from aiomisc.service.periodic import (
    PeriodicService,
)
from minos.common.broker import (
    Event,
)
from minos.common.configuration.config import (
    MinosConfig,
)
from minos.common.importlib import (
    import_module,
)
from minos.common.logs import (
    log,
)

from minos.networks.exceptions import (
    MinosNetworkException,
)


class MinosEventServer(Service):
    """
    Event Manager

    Consumer for the Broker ( at the moment only Kafka is supported )

    """

    __slots__ = "_tasks", "_db_dsn", "_handlers", "_topics", "_kafka_conn_data", "_broker_group_name"

    def __init__(self, *, conf: MinosConfig, **kwargs: t.Any):
        self._tasks = set()  # type: t.Set[asyncio.Task]
        self._db_dsn = (
            f"dbname={conf.events.queue.database} user={conf.events.queue.user} "
            f"password={conf.events.queue.password} host={conf.events.queue.host}"
        )
        self._handler = {
            item.name: {"controller": item.controller, "action": item.action} for item in conf.events.items
        }
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
        try:
            Event.from_avro_bytes(msg.value)
            await self.event_queue_add(msg.topic, msg.partition, msg.value)
        finally:
            pass

    async def handle_message(self, consumer: t.Any):
        """Message consumer.

        It consumes the messages and sends them for processing.

        Args:
            consumer: Kafka Consumer instance (at the moment only Kafka consumer is supported).
        """

        async for msg in consumer:
            await self.handle_single_message(msg)

    async def start(self) -> t.Any:
        self.start_event.set()
        log.debug("Event Consumer Manager: Started")
        # start the Service Event Consumer for Kafka
        consumer = AIOKafkaConsumer(
            loop=self.loop,
            group_id=self._broker_group_name,
            auto_offset_reset="latest",
            bootstrap_servers=self._kafka_conn_data,
        )

        await consumer.start()
        consumer.subscribe(self._topics)

        self.create_task(self.handle_message(consumer))


async def event_handler_table_creation(conf: MinosConfig):
    db_dsn = (
        f"dbname={conf.events.queue.database} user={conf.events.queue.user} "
        f"password={conf.events.queue.password} host={conf.events.queue.host}"
    )
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


class MinosEventHandlerPeriodicService(PeriodicService):
    """
    Periodic Service Event Handler

    """

    __slots__ = "_db_dsn", "_handlers", "_event_items", "_topics", "_conf"

    def __init__(self, *, conf: MinosConfig, **kwargs: t.Any):
        super().__init__(**kwargs)
        self._db_dsn = (
            f"dbname={conf.events.queue.database} user={conf.events.queue.user} "
            f"password={conf.events.queue.password} host={conf.events.queue.host}"
        )
        self._handlers = {
            item.name: {"controller": item.controller, "action": item.action} for item in conf.events.items
        }
        self._event_items = conf.events.items
        self._topics = list(self._handlers.keys())
        self._conf = conf

    def get_event_handler(self, topic: str) -> t.Callable:

        """Get Event instance to call.

        Gets the instance of the class and method to call.

        Args:
            topic: Kafka topic. Example: "TicketAdded"

        Raises:
            MinosNetworkException: topic TicketAdded have no controller/action configured, please review th configuration file.
        """
        for event in self._event_items:
            if event.name == topic:
                # the topic exist, get the controller and the action
                controller = event.controller
                action = event.action

                object_class = import_module(controller)
                log.debug(object_class())
                instance_class = object_class()
                class_method = getattr(instance_class, action)

                return class_method
        raise MinosNetworkException(
            f"topic {topic} have no controller/action configured, " f"please review th configuration file"
        )

    async def event_queue_checker(self):
        """Event Queue Checker and dispatcher.

        It is in charge of querying the database and calling the action according to the topic.

            1. Get periodically 10 records (or as many as defined in config > queue > records).
            2. Instantiate the action (asynchronous) by passing it the model.
            3. If the invoked function terminates successfully, remove the event from the database.

        Raises:
            Exception: An error occurred inserting record.
        """
        db_dsn = (
            f"dbname={self._conf.events.queue.database} user={self._conf.events.queue.user} "
            f"password={self._conf.events.queue.password} host={self._conf.events.queue.host}"
        )
        async with aiopg.create_pool(db_dsn) as pool:
            async with pool.acquire() as connect:
                async with connect.cursor() as cur:
                    await cur.execute(
                        "SELECT * FROM event_queue ORDER BY creation_date ASC LIMIT %d;"
                        % (self._conf.events.queue.records),
                    )
                    async for row in cur:
                        call_ok = False
                        try:
                            reply_on = self.get_event_handler(topic=row[1])
                            event_instance = Event.from_avro_bytes(row[3])
                            await reply_on(topic=row[1], event=event_instance)
                            call_ok = True
                        except:
                            call_ok = False
                        finally:
                            if call_ok:
                                # Delete from database If the event was sent successfully to Kafka.
                                async with connect.cursor() as cur2:
                                    await cur2.execute("DELETE FROM event_queue WHERE id=%d;" % row[0])

    async def callback(self):
        await self.event_queue_checker()
