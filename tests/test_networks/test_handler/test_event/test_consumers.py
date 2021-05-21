import unittest
from collections import (
    namedtuple,
)

from minos.common import (
    Event,
    MinosConfigException,
)
from minos.common.testing import (
    PostgresAsyncTestCase,
)
from minos.networks import (
    EventConsumer,
)
from tests.utils import (
    BASE_PATH,
    NaiveAggregate,
)


class TestEventServer(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def test_from_config(self):
        dispatcher = EventConsumer.from_config(config=self.config)
        self.assertIsInstance(dispatcher, EventConsumer)

    def test_from_config_raises(self):
        with self.assertRaises(MinosConfigException):
            EventConsumer.from_config()

    async def test_queue_add(self):
        model = NaiveAggregate(test_id=1, test=2, id=1, version=1)
        event_instance = Event(topic="TestEventQueueAdd", model=model.classname, items=[])
        bin_data = event_instance.avro_bytes
        Event.from_avro_bytes(bin_data)

        async with EventConsumer.from_config(config=self.config) as event_server:
            id = await event_server.queue_add(topic=event_instance.topic, partition=0, binary=bin_data)
            assert id > 0

    async def test_handle_message(self):
        async with EventConsumer.from_config(config=self.config) as event_server:
            model = NaiveAggregate(test_id=1, test=2, id=1, version=1)
            event_instance = Event(topic="TicketAdded", model=model.classname, items=[model])
            bin_data = event_instance.avro_bytes

            Mensaje = namedtuple("Mensaje", ["topic", "partition", "value"])

            async def consumer():
                yield Mensaje(topic="TicketAdded", partition=0, value=bin_data)

            await event_server.handle_message(consumer())

    async def test_handle_message_ko(self):
        async with EventConsumer.from_config(config=self.config) as event_server:
            bin_data = bytes(b"test")

            Mensaje = namedtuple("Mensaje", ["topic", "partition", "value"])

            async def consumer():
                yield Mensaje(topic="TicketAdded", partition=0, value=bin_data)

            await event_server.handle_message(consumer())


if __name__ == "__main__":
    unittest.main()
