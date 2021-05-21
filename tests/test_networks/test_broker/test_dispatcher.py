import unittest

from minos.common import (
    MinosConfigException,
)
from minos.common.testing import (
    PostgresAsyncTestCase,
)
from minos.networks import (
    ProducerDispatcher,
)
from tests.utils import (
    BASE_PATH,
)


class TestQueueDispatcher(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def test_from_config(self):
        dispatcher = ProducerDispatcher.from_config(config=self.config)
        self.assertIsInstance(dispatcher, ProducerDispatcher)

    def test_from_config_default(self):
        self.assertIsInstance(ProducerDispatcher.from_config(config=self.config), ProducerDispatcher)

    def test_from_config_raises(self):
        with self.assertRaises(MinosConfigException):
            ProducerDispatcher.from_config()

    async def test_select(self):
        dispatcher = ProducerDispatcher.from_config(config=self.config)
        await dispatcher.setup()
        self.assertEqual([], [v async for v in dispatcher.select()])

    async def test_send_to_kafka_ok(self):
        dispatcher = ProducerDispatcher.from_config(config=self.config)
        response = await dispatcher.publish(topic="TestKafkaSend", message=bytes())
        assert response is True


if __name__ == "__main__":
    unittest.main()
