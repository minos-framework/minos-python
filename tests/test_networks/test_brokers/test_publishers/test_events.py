import unittest
from unittest.mock import (
    AsyncMock,
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
    FAKE_AGGREGATE_DIFF,
)


class TestEventBroker(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def test_from_config_default(self):
        self.assertIsInstance(BrokerPublisher.from_config(config=self.config), BrokerPublisher)

    @unittest.skip
    def test_action(self):
        self.assertEqual("event", BrokerPublisher.ACTION)

    async def test_send(self):
        mock = AsyncMock(return_value=56)
        async with BrokerPublisher.from_config(config=self.config) as broker:
            broker.enqueue = mock
            identifier = await broker.send(FAKE_AGGREGATE_DIFF, topic="fake")

        self.assertEqual(56, identifier)
        self.assertEqual(1, mock.call_count)

        args = mock.call_args.args
        self.assertEqual("fake", args[0])
        expected = BrokerMessage("fake", FAKE_AGGREGATE_DIFF, service_name="Order")
        self.assertEqual(expected, Model.from_avro_bytes(args[1]))


if __name__ == "__main__":
    unittest.main()
