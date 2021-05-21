import unittest
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
    CommandReplyConsumer,
)
from tests.utils import (
    BASE_PATH,
    NaiveAggregate,
)


class TestCommandReplyServer(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def test_from_config(self):
        dispatcher = CommandReplyConsumer.from_config(config=self.config)
        self.assertIsInstance(dispatcher, CommandReplyConsumer)

    def test_from_config_raises(self):
        with self.assertRaises(MinosConfigException):
            CommandReplyConsumer.from_config()

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

        async with CommandReplyConsumer.from_config(config=self.config) as event_server:
            id = await event_server.queue_add(topic=event_instance.topic, partition=0, binary=bin_data)
            assert id > 0

    async def test_dispatch(self):
        async with CommandReplyConsumer.from_config(config=self.config) as event_server:
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
                yield Mensaje(topic="TicketAdded", partition=0, value=bin_data)

            event_server._consumer = consumer()

            await event_server.dispatch()

    async def test_handle_message(self):
        async with CommandReplyConsumer.from_config(config=self.config) as event_server:
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
        async with CommandReplyConsumer.from_config(config=self.config) as event_server:
            bin_data = bytes(b"test")

            Mensaje = namedtuple("Mensaje", ["topic", "partition", "value"])

            async def consumer():
                yield Mensaje(topic="AddOrder", partition=0, value=bin_data)

            await event_server.handle_message(consumer())


if __name__ == "__main__":
    unittest.main()
