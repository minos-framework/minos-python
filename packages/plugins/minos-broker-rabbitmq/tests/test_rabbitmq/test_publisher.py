import unittest
from unittest.mock import (
    patch,
)

from minos.common import (
    Config,
)
from minos.networks import (
    BrokerMessageV1,
    BrokerMessageV1Payload,
    BrokerPublisher,
)
from minos.plugins.rabbitmq import (
    RabbitMQBrokerPublisher,
)
from tests.utils import (
    CONFIG_FILE_PATH,
)


class TestRabbitMQBrokerPublisher(unittest.IsolatedAsyncioTestCase):
    def test_is_subclass(self):
        self.assertTrue(issubclass(RabbitMQBrokerPublisher, BrokerPublisher))

    def test_from_config(self):
        config = Config(CONFIG_FILE_PATH)
        broker_config = config.get_interface_by_name("broker")["common"]

        publisher = RabbitMQBrokerPublisher.from_config(config)

        self.assertIsInstance(publisher, RabbitMQBrokerPublisher)
        self.assertEqual(broker_config["host"], publisher.host)
        self.assertEqual(broker_config["port"], publisher.port)

    @patch("minos.plugins.rabbitmq.publisher.connect")
    async def test_send(self, connect_mock):
        message = BrokerMessageV1("foo", BrokerMessageV1Payload("bar"))

        async with RabbitMQBrokerPublisher.from_config(CONFIG_FILE_PATH) as publisher:
            await publisher.send(message)

            self.assertEqual(1, connect_mock.call_count)

    @patch("minos.plugins.rabbitmq.publisher.connect")
    async def test_destroy(self, destroy_mock):
        async with RabbitMQBrokerPublisher.from_config(CONFIG_FILE_PATH) as publisher:
            await publisher.destroy()

            self.assertEqual(1, destroy_mock.call_count)


if __name__ == "__main__":
    unittest.main()
