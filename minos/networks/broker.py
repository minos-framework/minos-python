# Copyright (C) 2020 Clariteia SL
#
# This file is part of minos framework.
#
# Minos framework can not be copied and/or distributed without the express
# permission of Clariteia SL.
import abc
import asyncio
import datetime
import typing as t

import aiomisc
import aiopg
from aiokafka import AIOKafkaProducer
from aiomisc.service.periodic import PeriodicService, Service
from minos.common import MinosModel, ModelRef
from minos.common.broker import MinosBaseBroker
from minos.common.configuration.config import MinosConfig
from minos.common.logs import log


class MinosBrokerDatabase:
    def get_connection(self, conf):
        conn = aiopg.connect(
            database=conf.events.queue.database,
            user=conf.events.queue.user,
            password=conf.events.queue.password,
            host=conf.events.queue.host,
            port=conf.events.queue.port,
        )
        return conn


class Aggregate(MinosModel):
    test_id: int


class EventModel(MinosModel):
    topic: str
    model: str
    # items: list[ModelRef[Aggregate]]
    items: list[str]


class CommandModel(MinosModel):
    topic: str
    model: str
    # items: list[ModelRef[Aggregate]]
    items: list[str]
    callback: str


class MinosEventBroker(MinosBaseBroker):
    def __init__(self, topic: str, config: MinosConfig):
        self.config = config
        self.topic = topic
        self._database()

    def _database(self):
        pass

    async def send(self, model: Aggregate):

        # TODO: Change
        event_instance = EventModel(topic=self.topic, model="Change", items=[str(model)])
        bin_data = event_instance.avro_bytes

        async with MinosBrokerDatabase().get_connection(self.config) as connect:
            async with connect.cursor() as cur:
                await cur.execute(
                    "INSERT INTO queue (topic, model, retry, creation_date, update_date) VALUES (%s, %s, %s, %s, %s) RETURNING queue_id;",
                    (event_instance.topic, bin_data, 0, datetime.datetime.now(), datetime.datetime.now(),),
                )

                queue_id = await cur.fetchone()
                affected_rows = cur.rowcount

        return affected_rows, queue_id[0]


class MinosCommandBroker(MinosBaseBroker):
    def __init__(self, topic: str, config: MinosConfig):
        self.config = config
        self.topic = topic
        self._database()

    def _database(self):
        pass

    async def send(self, model: Aggregate, callback: t.Callable):
        # TODO: Change
        event_instance = CommandModel(topic=self.topic, model="Change", items=[str(model)], callback=callback)
        bin_data = event_instance.avro_bytes

        async with MinosBrokerDatabase().get_connection(self.config) as connect:
            async with connect.cursor() as cur:
                await cur.execute(
                    "INSERT INTO queue (topic, model, retry, creation_date, update_date) VALUES (%s, %s, %s, %s, %s) RETURNING queue_id;",
                    (event_instance.topic, bin_data, 0, datetime.datetime.now(), datetime.datetime.now(),),
                )

                queue_id = await cur.fetchone()
                affected_rows = cur.rowcount

        return affected_rows, queue_id[0]


class BrokerDatabaseInitializer(Service):
    async def start(self):
        # Send signal to entrypoint for continue running
        self.start_event.set()

        async with MinosBrokerDatabase().get_connection(self.config) as connect:
            async with connect.cursor() as cur:
                await cur.execute(
                    'CREATE TABLE IF NOT EXISTS "queue" ("queue_id" SERIAL NOT NULL PRIMARY KEY, '
                    '"topic" VARCHAR(255) NOT NULL, "model" BYTEA NOT NULL, "retry" INTEGER NOT NULL, '
                    '"creation_date" TIMESTAMP NOT NULL, "update_date" TIMESTAMP NOT NULL);'
                )

        await self.stop(self)


class EventBrokerQueueDispatcher(PeriodicService):
    async def callback(self):
        async with MinosBrokerDatabase().get_connection(self.config) as connect:
            async with connect.cursor() as cur:
                await cur.execute("SELECT * FROM queue WHERE retry <= 2 ORDER BY creation_date ASC LIMIT 10;")
                async for row in cur:

                    log.debug(row)
                    log.debug("id = %s", row[0])
                    log.debug("topic = %s", row[1])
                    log.debug("model  = %s", row[2])
                    log.debug("retry  = %s", row[3])
                    log.debug("creation_date  = %s", row[4])
                    log.debug("update_date  = %s", row[5])

                    sent_to_kafka = await self._send_to_kafka(topic=row[1], message=row[2])
                    if sent_to_kafka:
                        # Delete from database If the event was sent successfully to Kafka.
                        async with connect.cursor() as cur2:
                            await cur2.execute("DELETE FROM queue WHERE queue_id=%d;" % row[0])
                    else:
                        # Update queue retry column. Increase by 1.
                        async with connect.cursor() as cur3:
                            await cur3.execute("UPDATE queue SET retry = retry + 1 WHERE queue_id=%d;" % row[0])

            connect.commit()

    async def _send_to_kafka(self, topic: str, message: bytes):
        flag = False
        producer = AIOKafkaProducer(bootstrap_servers="localhost:9092")
        # Get cluster layout and initial topic/partition leadership information
        await producer.start()
        try:
            # Produce message
            await producer.send_and_wait(topic, message)
            flag = True
        except Exception as e:
            flag = False
        finally:
            # Wait for all pending messages to be delivered or expire.
            await producer.stop()

        return flag
