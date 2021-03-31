import asyncio
import logging

import pytest
import string
from aiokafka import AIOKafkaProducer
import random

from aiomisc.log import basic_config

from minos.networks.event import MinosEventServer
from tests.conftest import CallBackReturn


@pytest.fixture()
def config():
    return {
        "controller": "tests.controllers.RootController",
        "group": "OrdersGroup",
        "host": "localhost",
        "port": 8900,
        "kafka_host": "localhost",
        "kafka_port": 9092,
        "db_events_path": "./tests/events_db.lmdb"
    }


def TicketAddedCallback(message: bytes):
    CallBackReturn.content = message.decode('utf-8')


def TicketRemoved(message: bytes):
    CallBackReturn.content = message.decode('utf-8')


@pytest.fixture()
def services(config):
    handlers_test = {
        'TicketAdded': TicketAddedCallback,
        'TicketRemoved': TicketRemoved
    }

    return [MinosEventServer(conf=config, handlers=handlers_test)]


async def test_producer_kafka(loop):
    basic_config(
        level=logging.INFO,
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
    assert CallBackReturn.content == string_to_send

    other_string_to_send = ''.join(random.choices(string.ascii_uppercase + string.digits, k=40))
    await producer.send_and_wait("TicketRemoved", other_string_to_send.encode())
    await asyncio.sleep(1)
    assert CallBackReturn.content == other_string_to_send
    await producer.stop()
