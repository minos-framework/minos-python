import pytest
import string
from aiokafka import AIOKafkaProducer
import random

from minos.networks.event import MinosEventServer


@pytest.fixture()
def config():
    return {
        "controller": "tests.controllers.RootController",
        "group": "OrdersGroup",
        "host": "localhost",
        "port": 8900,
        "kafka_host": "localhost",
        "kafka_port": 9092,
        "db_events_path": "./events_db.lmdb"
    }


@pytest.fixture()
def services(config):
    return [MinosEventServer(conf=config, topics=['TicketAdded', 'TicketRemoved'])]


async def test_producer_kafka(loop):
    producer = AIOKafkaProducer(loop=loop, bootstrap_servers='localhost:9092')
    # Get cluster layout and topic/partition allocation
    await producer.start()
    # Produce messages
    string_to_send = ''.join(random.choices(string.ascii_uppercase + string.digits, k=20))
    res = await producer.send_and_wait("TicketAdded", string_to_send.encode())
    print(res)
    # await asyncio.sleep(1)

    other_string_to_send = ''.join(random.choices(string.ascii_uppercase + string.digits, k=40))
    res = await producer.send_and_wait("TicketRemoved", other_string_to_send.encode())
    print(res)
    # await asyncio.sleep(1)
    producer.stop()
