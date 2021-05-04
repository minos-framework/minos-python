import time
from collections import namedtuple

from minos.common.testing import (
    PostgresAsyncTestCase,
)
from minos.networks import (
    MinosEventServer,
)
from tests.utils import (
    BASE_PATH,
    NaiveAggregate,
)
from minos.common import (
    Event
)
from aiokafka import (
    AIOKafkaConsumer,
    AIOKafkaProducer,
)


async def kafka_producer(config):

    # basic_config(
    #    level=logging.INFO,
    #    buffered=True,
    #    log_format='color',
    #    flush_interval=2
    # )
    kafka_conn_data = f"{config.events.broker.host}:{config.events.broker.port}"
    producer = AIOKafkaProducer(bootstrap_servers=kafka_conn_data)
    # Get cluster layout and topic/partition allocation
    await producer.start()
    # Produce messages

    model = NaiveAggregate(test_id=1, test=2, id=1, version=1)
    event_instance = Event(topic="TicketAdded", model=model.classname, items=[model])
    bin_data = event_instance.avro_bytes

    model2 = NaiveAggregate(test_id=2, test=2, id=1, version=1)
    event_instance_2 = Event(topic="TicketDeleted", model=model2.classname, items=[model2])
    bin_data2 = event_instance_2.avro_bytes

    await producer.send_and_wait(event_instance.topic, bin_data)
    time.sleep(1)
    await producer.send_and_wait(event_instance_2.topic, bin_data2)
    time.sleep(1)
    await producer.stop()


class TestEventServer(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def test_from_config(self):
        dispatcher = MinosEventServer.from_config(config=self.config)
        self.assertIsInstance(dispatcher, MinosEventServer)

    async def test_none_config(self):
        event_server = MinosEventServer.from_config(config=None)

        self.assertIsNone(event_server)

    async def test_event_queue_add(self):
        model = NaiveAggregate(test_id=1, test=2, id=1, version=1)
        event_instance = Event(topic="TestEventQueueAdd", model=model.classname, items=[])
        bin_data = event_instance.avro_bytes
        Event.from_avro_bytes(bin_data)

        event_server = MinosEventServer.from_config(config=self.config)
        await event_server.setup()

        affected_rows, id = await event_server.event_queue_add(topic=event_instance.topic, partition=0, binary=bin_data)

        assert affected_rows == 1
        assert id > 0

    async def test_handle_message(self):
        event_server = MinosEventServer.from_config(config=self.config)
        await event_server.setup()

        model = NaiveAggregate(test_id=1, test=2, id=1, version=1)
        event_instance = Event(topic="TicketAdded", model=model.classname, items=[model])
        bin_data = event_instance.avro_bytes

        Mensaje = namedtuple("Mensaje",["topic", "partition", "value"])

        async def consumer():
            yield Mensaje(topic="TicketAdded",partition=0, value=bin_data)

        await event_server.handle_message(consumer())

