import unittest
from unittest.mock import (
    AsyncMock,
)
from uuid import (
    uuid4,
)

from minos.common import (
    Command,
)
from minos.common.testing import (
    PostgresAsyncTestCase,
)
from minos.networks import (
    REPLY_TOPIC_CONTEXT_VAR,
    CommandBroker,
)
from tests.utils import (
    BASE_PATH,
    FakeModel,
)


class TestCommandBroker(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def test_from_config_default(self):
        broker = CommandBroker.from_config(config=self.config)
        self.assertIsInstance(broker, CommandBroker)

    def test_action(self):
        self.assertEqual("command", CommandBroker.ACTION)

    def test_default_reply_topic(self):
        broker = CommandBroker.from_config(config=self.config)
        self.assertEqual("OrderReply", broker.default_reply_topic)

    async def test_send(self):
        mock = AsyncMock(return_value=56)
        saga = uuid4()

        async with CommandBroker.from_config(config=self.config) as broker:
            broker.enqueue = mock
            identifier = await broker.send(FakeModel("foo"), "fake", saga, "ekaf")

        self.assertEqual(56, identifier)
        self.assertEqual(1, mock.call_count)

        args = mock.call_args.args
        self.assertEqual("fake", args[0])
        self.assertEqual(Command("fake", FakeModel("foo"), saga, "ekaf"), Command.from_avro_bytes(args[1]))

    async def test_send_with_default_reply_topic(self):
        mock = AsyncMock(return_value=56)
        saga = uuid4()

        async with CommandBroker.from_config(config=self.config) as broker:
            broker.enqueue = mock
            identifier = await broker.send(FakeModel("foo"), "fake", saga)

        self.assertEqual(56, identifier)
        self.assertEqual(1, mock.call_count)

        args = mock.call_args.args
        self.assertEqual("fake", args[0])
        self.assertEqual(Command("fake", FakeModel("foo"), saga, "OrderReply"), Command.from_avro_bytes(args[1]))

    async def test_send_with_reply_topic_context_var(self):
        mock = AsyncMock(return_value=56)
        saga = uuid4()

        REPLY_TOPIC_CONTEXT_VAR.set("onetwothree")

        async with CommandBroker.from_config(config=self.config) as broker:
            broker.enqueue = mock
            identifier = await broker.send(FakeModel("foo"), "fake", saga)

        self.assertEqual(56, identifier)
        self.assertEqual(1, mock.call_count)

        args = mock.call_args.args
        self.assertEqual("fake", args[0])
        self.assertEqual(Command("fake", FakeModel("foo"), saga, "onetwothree"), Command.from_avro_bytes(args[1]))

    async def test_send_with_user(self):
        mock = AsyncMock(return_value=56)
        saga = uuid4()
        user = uuid4()

        async with CommandBroker.from_config(config=self.config) as broker:
            broker.enqueue = mock
            identifier = await broker.send(FakeModel("foo"), "fake", saga, "ekaf", user)

        self.assertEqual(56, identifier)
        self.assertEqual(1, mock.call_count)

        args = mock.call_args.args
        self.assertEqual("fake", args[0])
        self.assertEqual(Command("fake", FakeModel("foo"), saga, "ekaf", user), Command.from_avro_bytes(args[1]))


if __name__ == "__main__":
    unittest.main()
