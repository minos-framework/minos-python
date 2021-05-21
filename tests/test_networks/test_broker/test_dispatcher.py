import unittest

from minos.common import (
    MinosConfigException,
)
from minos.common.testing import (
    PostgresAsyncTestCase,
)
from minos.networks import (
    Producer,
)
from tests.utils import (
    BASE_PATH,
)


class TestQueueDispatcher(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def test_from_config(self):
        dispatcher = Producer.from_config(config=self.config)
        self.assertIsInstance(dispatcher, Producer)

    def test_from_config_default(self):
        self.assertIsInstance(Producer.from_config(config=self.config), Producer)

    def test_from_config_raises(self):
        with self.assertRaises(MinosConfigException):
            Producer.from_config()

    async def test_select(self):
        dispatcher = Producer.from_config(config=self.config)
        await dispatcher.setup()
        self.assertEqual([], [v async for v in dispatcher.select()])

    async def test_send_to_kafka_ok(self):
        dispatcher = Producer.from_config(config=self.config)
        response = await dispatcher.publish(topic="TestKafkaSend", message=bytes())
        assert response is True


if __name__ == "__main__":
    unittest.main()
