import asyncio
import logging
import random
import string

import pytest
from aiokafka import (
    AIOKafkaConsumer,
    AIOKafkaProducer,
)
from aiomisc.log import (
    basic_config,
)
from minos.common import (
    Aggregate,
)
from minos.common.broker import (
    Event,
)
from minos.common.configuration.config import (
    MinosConfig,
)
from minos.common.logs import (
    log,
)
from minos.networks.event import (
    EventHandlerDatabaseInitializer,
    MinosEventHandlerPeriodicService,
    MinosEventServer,
)
from tests.database_testcase import (
    EventHandlerPostgresAsyncTestCase,
)


@pytest.fixture()
def config():
    return MinosConfig(path="./tests/test_config.yml")


@pytest.fixture()
def services(config):
    return [
        EventHandlerDatabaseInitializer(config=config),
        MinosEventServer(conf=config),
        MinosEventHandlerPeriodicService(interval=0.5, delay=0, conf=config),
    ]


class AggregateTest(Aggregate):
    test: int


class TestPostgreSqlMinosEventHandler(EventHandlerPostgresAsyncTestCase):
    async def test_database_connection(self):
        database = await self._database()
        async with database as connect:
            assert database.closed == 0

    async def test_if_queue_table_exists(self):
        database = await self._database()
        async with database as connect:
            async with connect.cursor() as cur:

                await cur.execute(
                    "SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'event_queue';"
                )
                ret = []
                async for row in cur:
                    ret.append(row)

        assert ret == [(1,)]

    async def test_event_queue_add(self):
        model = AggregateTest(test_id=1, test=2, id=1, version=1)
        event_instance = Event(topic="TestEventQueueAdd", model=model.classname, items=[])
        bin_data = event_instance.avro_bytes
        Event.from_avro_bytes(bin_data)

        m = MinosEventServer(conf=self._broker_config())
        affected_rows, id = await m.event_queue_add(topic=event_instance.topic, partition=0, binary=bin_data)

        assert affected_rows == 1
        assert id > 0

    async def test_get_event_handler(self):
        model = AggregateTest(test_id=1, test=2, id=1, version=1)
        event_instance = Event(topic="TestEventQueueAdd", model=model.classname, items=[])
        m = MinosEventHandlerPeriodicService(conf=self._broker_config(), interval=0.5)

        cls = m.get_event_handler(topic="TicketAdded")
        result = await cls(topic="TicketAdded", event=event_instance)

        assert result == "request_added"

    async def test_event_queue_checker(self):
        model = AggregateTest(test_id=1, test=2, id=1, version=1)
        event_instance = Event(topic="TicketAdded", model=model.classname, items=[])
        bin_data = event_instance.avro_bytes
        Event.from_avro_bytes(bin_data)

        m = MinosEventServer(conf=self._broker_config())
        affected_rows, id = await m.event_queue_add(topic=event_instance.topic, partition=0, binary=bin_data)

        assert affected_rows == 1
        assert id > 0

        database = await self._database()
        async with database as connect:
            async with connect.cursor() as cur:
                await cur.execute("SELECT COUNT(*) FROM event_queue WHERE id=%d" % (id))
                records = await cur.fetchone()

        assert records[0] == 1

        m = MinosEventHandlerPeriodicService(conf=self._broker_config(), interval=0.5)
        await m.event_queue_checker()

        database = await self._database()
        async with database as connect:
            async with connect.cursor() as cur:
                await cur.execute("SELECT COUNT(*) FROM event_queue WHERE id=%d" % (id))
                records = await cur.fetchone()

        assert records[0] == 0


async def test_producer_kafka(config, loop):

    # basic_config(
    #    level=logging.INFO,
    #    buffered=True,
    #    log_format='color',
    #    flush_interval=2
    # )
    kafka_conn_data = f"{config.events.broker.host}:{config.events.broker.port}"
    producer = AIOKafkaProducer(loop=loop, bootstrap_servers=kafka_conn_data)
    # Get cluster layout and topic/partition allocation
    await producer.start()
    # Produce messages

    model = AggregateTest(test_id=1, test=2, id=1, version=1)
    event_instance = Event(topic="TicketAdded", model=model.classname, items=[model])
    bin_data = event_instance.avro_bytes

    model2 = AggregateTest(test_id=2, test=2, id=1, version=1)
    event_instance_2 = Event(topic="TicketDeleted", model=model2.classname, items=[model2])
    bin_data2 = event_instance_2.avro_bytes

    for i in range(0, 10):
        await producer.send_and_wait(event_instance.topic, bin_data)
        await producer.send_and_wait(event_instance_2.topic, bin_data2)

    await producer.stop()


"""
async def test_consumer_kafka(config,loop):
    handler = {item.name: {'controller': item.controller, 'action': item.action}
                     for item in config.events.items}
    topics = list(handler.keys())
    kafka_conn_data = f"{config.events.broker.host}:{config.events.broker.port}"
    broker_group_name = f"event_{config.service.name}"

    m = MinosEventServer(conf=config)
    consumer = AIOKafkaConsumer(
                                loop=loop,
                                group_id=broker_group_name,
                                auto_offset_reset="latest",
                                bootstrap_servers=kafka_conn_data, consumer_timeout_ms=500
                                )

    await consumer.start()
    consumer.subscribe(topics)

    for i in range(0, 2):
        msg = await consumer.getone()
        await m.handle_single_message(msg)

    await consumer.stop()
"""
