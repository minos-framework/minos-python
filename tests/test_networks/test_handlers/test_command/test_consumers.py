import unittest
from collections import (
    namedtuple,
)
from unittest.mock import (
    MagicMock,
)

from minos.common import (
    Command,
    MinosConfigException,
)
from minos.common.testing import (
    PostgresAsyncTestCase,
)
from minos.networks import (
    CommandConsumer,
)
from tests.utils import (
    BASE_PATH,
    FakeConsumer,
    NaiveAggregate,
)


class TestCommandServer(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def test_from_config(self):
        dispatcher = CommandConsumer.from_config(config=self.config)
        self.assertIsInstance(dispatcher, CommandConsumer)

    def test_from_config_raises(self):
        with self.assertRaises(MinosConfigException):
            CommandConsumer.from_config()

    async def test_queue_add(self):
        model = NaiveAggregate(test_id=1, test=2, id=1, version=1)
        event_instance = Command(
            topic="AddOrder",
            model=model.classname,
            items=[],
            saga_id="43434jhij",
            task_id="juhjh34",
            reply_on="mkk2334",
        )
        bin_data = event_instance.avro_bytes
        Command.from_avro_bytes(bin_data)

        async with CommandConsumer.from_config(config=self.config) as handler:
            id = await handler.queue_add(topic=event_instance.topic, partition=0, binary=bin_data)
            assert id > 0

    async def test_dispatch(self):
        handler = CommandConsumer.from_config(config=self.config)

        async def _fn():
            return FakeConsumer()

        handler._build_kafka_consumer = MagicMock(side_effect=_fn)
        async with handler:
            await handler.dispatch()

    async def test_handle_message(self):
        async with CommandConsumer.from_config(config=self.config) as handler:
            model = NaiveAggregate(test_id=1, test=2, id=1, version=1)
            event_instance = Command(
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

            await handler.handle_message(consumer())

    async def test_handle_message_ko(self):
        async with CommandConsumer.from_config(config=self.config) as handler:
            bin_data = bytes(b"test")

            Mensaje = namedtuple("Mensaje", ["topic", "partition", "value"])

            async def consumer():
                yield Mensaje(topic="TicketAdded", partition=0, value=bin_data)

            await handler.handle_message(consumer())


if __name__ == "__main__":
    unittest.main()
