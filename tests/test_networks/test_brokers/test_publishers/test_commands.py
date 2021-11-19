import unittest
from unittest.mock import (
    AsyncMock,
)
from uuid import (
    uuid4,
)

from minos.common import (
    Model,
)
from minos.common.testing import (
    PostgresAsyncTestCase,
)
from minos.networks import (
    BrokerMessage,
    BrokerPublisher,
)
from tests.utils import (
    BASE_PATH,
    FakeModel,
)


class TestCommandBroker(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def test_from_config_default(self):
        broker = BrokerPublisher.from_config(config=self.config)
        self.assertIsInstance(broker, BrokerPublisher)

    def test_action(self):
        self.assertEqual("command", BrokerPublisher.ACTION)

    async def test_send(self):
        mock = AsyncMock(return_value=56)
        saga = uuid4()

        async with BrokerPublisher.from_config(config=self.config) as broker:
            broker.enqueue = mock
            identifier = await broker.send(FakeModel("foo"), "fake", saga=saga, reply_topic="ekaf")

        self.assertEqual(56, identifier)
        self.assertEqual(1, mock.call_count)

        args = mock.call_args.args
        self.assertEqual("fake", args[0])
        expected = BrokerMessage("fake", FakeModel("foo"), saga=saga, reply_topic="ekaf", service_name="Order")
        self.assertEqual(expected, Model.from_avro_bytes(args[1]))

    async def test_send_with_user(self):
        mock = AsyncMock(return_value=56)
        saga = uuid4()
        user = uuid4()

        async with BrokerPublisher.from_config(config=self.config) as broker:
            broker.enqueue = mock
            identifier = await broker.send(FakeModel("foo"), "fake", saga=saga, reply_topic="ekaf", user=user)

        self.assertEqual(56, identifier)
        self.assertEqual(1, mock.call_count)

        args = mock.call_args.args
        self.assertEqual("fake", args[0])
        expected = BrokerMessage(
            "fake", FakeModel("foo"), saga=saga, reply_topic="ekaf", user=user, service_name="Order"
        )
        self.assertEqual(expected, Model.from_avro_bytes(args[1]))


if __name__ == "__main__":
    unittest.main()
