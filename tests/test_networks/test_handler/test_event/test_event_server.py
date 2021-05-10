from collections import namedtuple
from minos.common import Event
from minos.common.testing import PostgresAsyncTestCase
from minos.networks import MinosEventHandlerServer
from tests.utils import (
    BASE_PATH,
    NaiveAggregate,
)


class TestEventServer(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def test_from_config(self):
        dispatcher = MinosEventHandlerServer.from_config(config=self.config)
        self.assertIsInstance(dispatcher, MinosEventHandlerServer)

    async def test_none_config(self):
        event_server = MinosEventHandlerServer.from_config(config=None)

        self.assertIsNone(event_server)

    async def test_queue_add(self):
        model = NaiveAggregate(test_id=1, test=2, id=1, version=1)
        event_instance = Event(topic="TestEventQueueAdd", model=model.classname, items=[])
        bin_data = event_instance.avro_bytes
        Event.from_avro_bytes(bin_data)

        event_server = MinosEventHandlerServer.from_config(config=self.config)
        await event_server.setup()

        affected_rows, id = await event_server.queue_add(topic=event_instance.topic, partition=0, binary=bin_data)

        assert affected_rows == 1
        assert id > 0

    async def test_handle_message(self):
        event_server = MinosEventHandlerServer.from_config(config=self.config)
        await event_server.setup()

        model = NaiveAggregate(test_id=1, test=2, id=1, version=1)
        event_instance = Event(topic="TicketAdded", model=model.classname, items=[model])
        bin_data = event_instance.avro_bytes

        Mensaje = namedtuple("Mensaje", ["topic", "partition", "value"])

        async def consumer():
            yield Mensaje(topic="TicketAdded", partition=0, value=bin_data)

        await event_server.handle_message(consumer())

    async def test_handle_message_ko(self):
        event_server = MinosEventHandlerServer.from_config(config=self.config)
        await event_server.setup()

        bin_data = bytes(b"test")

        Mensaje = namedtuple("Mensaje", ["topic", "partition", "value"])

        async def consumer():
            yield Mensaje(topic="TicketAdded", partition=0, value=bin_data)

        await event_server.handle_message(consumer())
