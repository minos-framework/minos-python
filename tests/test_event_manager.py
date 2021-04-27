import asyncio
import logging

import pytest
import string
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
import random

from aiomisc.log import basic_config
from minos.common.configuration.config import MinosConfig

from minos.networks.event import MinosEventServer, EventHandlerDatabaseInitializer
from tests.database_testcase import EventHandlerPostgresAsyncTestCase

@pytest.fixture()
def config():
 return MinosConfig(path='./tests/test_config.yaml')


@pytest.fixture()
def services(config):
 return [EventHandlerDatabaseInitializer(config=config), MinosEventServer(conf=config)]


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


async def test_producer_kafka(loop):
    basic_config(
        level=logging.DEBUG,
        buffered=True,
        log_format='color',
        flush_interval=2
    )

    producer = AIOKafkaProducer(loop=loop, bootstrap_servers='localhost:9092')
    # Get cluster layout and topic/partition allocation
    await producer.start()
    # Produce messages
    string_to_send = ''.join(random.choices(string.ascii_uppercase + string.digits, k=20))
    await producer.send_and_wait("TicketAdded", string_to_send.encode())
    await asyncio.sleep(1)

    other_string_to_send = ''.join(random.choices(string.ascii_uppercase + string.digits, k=40))
    await producer.send_and_wait("TicketDeleted", other_string_to_send.encode())
    await asyncio.sleep(1)
    await producer.stop()


async def test_event_handle_message(config, loop):
    handler = {item.name: {'controller': item.controller, 'action': item.action}
                     for item in config.events.items}
    topics = list(handler.keys())
    kafka_conn_data = f"{config.events.broker.host}:{config.events.broker.port}"
    broker_group_name = f"event_{config.service.name}"

    m = MinosEventServer(conf=config)
    consumer = AIOKafkaConsumer(loop=loop,
                                group_id=broker_group_name,
                                auto_offset_reset="latest",
                                bootstrap_servers=kafka_conn_data,
                                )

    await consumer.start()
    consumer.subscribe(topics)

    await m.handle_message(consumer)
