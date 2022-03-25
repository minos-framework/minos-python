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

    def test_constructor(self):
        publisher = RabbitMQBrokerPublisher()
        self.assertEqual("localhost", publisher.host)
        self.assertEqual(5672, publisher.port)

    def test_from_config(self):
        config = Config(CONFIG_FILE_PATH)
        broker_config = config.get_interface_by_name("broker")["common"]

        publisher = RabbitMQBrokerPublisher.from_config(config)

        self.assertIsInstance(publisher, RabbitMQBrokerPublisher)
        self.assertEqual(broker_config["host"], publisher.host)
        self.assertEqual(broker_config["port"], publisher.port)

    async def test_send(self):
        async with RabbitMQBrokerPublisher.from_config(CONFIG_FILE_PATH) as publisher:
            with patch("aio_pika.Exchange.publish") as mock:
                await publisher.send(BrokerMessageV1("foo1", BrokerMessageV1Payload("bar")))
                await publisher.send(BrokerMessageV1("foo2", BrokerMessageV1Payload("bar")))
                await publisher.send(BrokerMessageV1("foo1", BrokerMessageV1Payload("bar")))

        self.assertEqual(3, mock.call_count)


if __name__ == "__main__":
    unittest.main()
