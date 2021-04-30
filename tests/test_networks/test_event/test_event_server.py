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

    for i in range(0, 10):
        await producer.send_and_wait(event_instance.topic, bin_data)
        await producer.send_and_wait(event_instance_2.topic, bin_data2)

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

    async def test_consumer_kafka(self):
        await kafka_producer(self.config)
        handler = {item.name: {'controller': item.controller, 'action': item.action}
                   for item in self.config.events.items}
        topics = list(handler.keys())
        kafka_conn_data = f"{self.config.events.broker.host}:{self.config.events.broker.port}"
        broker_group_name = f"event_{self.config.service.name}"

        event_server = MinosEventServer.from_config(config=self.config)
        await event_server.setup()

        consumer = AIOKafkaConsumer(
            loop=None,
            group_id=broker_group_name,
            auto_offset_reset="latest",
            bootstrap_servers=kafka_conn_data, consumer_timeout_ms=500
        )

        await consumer.start()
        consumer.subscribe(topics)

        msg = await consumer.getone()
        affected_rows, id = await event_server.handle_single_message(msg)
        assert affected_rows > 0
        assert id > 0

        await consumer.stop()

    """
    async def test_handle_message(self):
        await kafka_producer(self.config)
        handler = {item.name: {'controller': item.controller, 'action': item.action}
                   for item in self.config.events.items}
        topics = list(handler.keys())
        kafka_conn_data = f"{self.config.events.broker.host}:{self.config.events.broker.port}"
        broker_group_name = f"event_{self.config.service.name}"

        event_server = MinosEventServer.from_config(config=self.config)
        await event_server.setup()

        consumer = AIOKafkaConsumer(
            loop=None,
            group_id=broker_group_name,
            auto_offset_reset="latest",
            bootstrap_servers=kafka_conn_data, consumer_timeout_ms=500
        )

        await consumer.start()
        consumer.subscribe(topics)

        await event_server.handle_message(consumer)

        await consumer.stop()
    """
