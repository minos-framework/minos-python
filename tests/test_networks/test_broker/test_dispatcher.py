import unittest

from minos.common.testing import PostgresAsyncTestCase
from minos.networks import MinosQueueDispatcher
from tests.utils import BASE_PATH


class TestQueueDispatcher(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def test_from_config(self):
        dispatcher = MinosQueueDispatcher.from_config(config=self.config)
        self.assertIsInstance(dispatcher, MinosQueueDispatcher)

    async def test_send_to_kafka_ok(self):
        dispatcher = MinosQueueDispatcher.from_config(config=self.config)
        response = await dispatcher.publish(topic="TestKafkaSend", message=bytes())
        assert response is True


if __name__ == "__main__":
    unittest.main()
