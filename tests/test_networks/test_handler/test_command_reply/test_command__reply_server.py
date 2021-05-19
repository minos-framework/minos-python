from collections import (
    namedtuple,
)

from minos.common import (
    CommandReply,
    MinosConfigException,
)
from minos.common.testing import (
    PostgresAsyncTestCase,
)
from minos.networks import (
    MinosCommandReplyHandlerServer,
)
from tests.utils import (
    BASE_PATH,
    NaiveAggregate,
)


class TestCommandReplyServer(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def test_from_config(self):
        dispatcher = MinosCommandReplyHandlerServer.from_config(config=self.config)
        self.assertIsInstance(dispatcher, MinosCommandReplyHandlerServer)

    def test_from_config_default(self):
        with self.config:
            self.assertIsInstance(MinosCommandReplyHandlerServer.from_config(), MinosCommandReplyHandlerServer)

    def test_from_config_raises(self):
        with self.assertRaises(MinosConfigException):
            MinosCommandReplyHandlerServer.from_config()

    async def test_queue_add(self):
        model = NaiveAggregate(test_id=1, test=2, id=1, version=1)
        event_instance = CommandReply(
            topic="AddOrder",
            model=model.classname,
            items=[],
            saga_id="43434jhij",
            task_id="juhjh34",
            reply_on="mkk2334",
        )
        bin_data = event_instance.avro_bytes
        CommandReply.from_avro_bytes(bin_data)

        event_server = MinosCommandReplyHandlerServer.from_config(config=self.config)
        await event_server.setup()

        id = await event_server.queue_add(topic=event_instance.topic, partition=0, binary=bin_data)
        assert id > 0

    async def test_handle_message(self):
        event_server = MinosCommandReplyHandlerServer.from_config(config=self.config)
        await event_server.setup()

        model = NaiveAggregate(test_id=1, test=2, id=1, version=1)
        event_instance = CommandReply(
            topic="AddOrder",
            model=model.classname,
            items=[],
            saga_id="43434jhij",
            task_id="juhjh34",
            reply_on="mkk2334",
        )
        bin_data = event_instance.avro_bytes

        Mensaje = namedtuple("Mensaje", ["topic", "partition", "value"])

        async def consumer():
            yield Mensaje(topic="AddOrder", partition=0, value=bin_data)

        await event_server.handle_message(consumer())

    async def test_handle_message_ko(self):
        event_server = MinosCommandReplyHandlerServer.from_config(config=self.config)
        await event_server.setup()

        bin_data = bytes(b"test")

        Mensaje = namedtuple("Mensaje", ["topic", "partition", "value"])

        async def consumer():
            yield Mensaje(topic="AddOrder", partition=0, value=bin_data)

        await event_server.handle_message(consumer())
