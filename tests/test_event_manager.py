import asyncio

import pytest
import string
from aiokafka import AIOKafkaProducer
import random
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


def TicketAddedCallback(message):
    CallBackReturn.content = message


def TicketRemoved(message):
    CallBackReturn.content = message


@pytest.fixture()
def services(config):
    handlers_test = {
        'TicketAdded': TicketAddedCallback,
        'TicketRemoved': TicketRemoved
    }

    return [MinosEventServer(conf=config, handlers=handlers_test)]


async def test_producer_kafka(loop):
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
    producer.stop()
