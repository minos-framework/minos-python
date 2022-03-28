import unittest
from collections import (
    namedtuple,
)
from unittest.mock import (
    patch,
)

from minos.common import (
    Config,
    ModelType,
)
from minos.networks import (
    BrokerSubscriber,
)
from minos.plugins.rabbitmq import (
    RabbitMQBrokerSubscriber,
    RabbitMQBrokerSubscriberBuilder,
)
from tests.utils import (
    CONFIG_FILE_PATH,
    FakeAsyncIterator,
)

_Foo = ModelType.build("_Foo", {"bar": str})
_ConsumerMessage = namedtuple("_ConsumerMessage", ["body"])


class TestRabbitMQBrokerSubscriber(unittest.IsolatedAsyncioTestCase):
    def test_is_subclass(self):
        self.assertTrue(issubclass(RabbitMQBrokerSubscriber, BrokerSubscriber))

    def test_constructor(self):
        subscriber = RabbitMQBrokerSubscriber({"foo", "bar"})
        self.assertEqual({"foo", "bar"}, subscriber.topics)
        self.assertEqual("localhost", subscriber.host)
        self.assertEqual(5672, subscriber.port)
        self.assertEqual("guest", subscriber.user)
        self.assertEqual("guest", subscriber.password)

    async def test_from_config(self):
        config = Config(CONFIG_FILE_PATH)
        broker_config = config.get_interface_by_name("broker")["common"]
        async with RabbitMQBrokerSubscriber.from_config(config, topics={"foo", "bar"}) as subscriber:
            self.assertEqual(broker_config["host"], subscriber.host)
            self.assertEqual(broker_config["port"], subscriber.port)
            self.assertEqual({"foo", "bar"}, subscriber.topics)

    async def test_receive(self):
        message = _ConsumerMessage(_Foo("foobar").avro_bytes)

        async with RabbitMQBrokerSubscriber.from_config(CONFIG_FILE_PATH, topics={"foo", "bar"}) as subscriber:
            with patch("aio_pika.Queue.iterator", return_value=FakeAsyncIterator([message])):
                observed = await subscriber.receive()

        self.assertEqual(_Foo("foobar"), observed)


class TestRabbitMQBrokerSubscriberBuilder(unittest.TestCase):
    def setUp(self) -> None:
        self.config = Config(CONFIG_FILE_PATH)

    def test_with_config(self):
        builder = RabbitMQBrokerSubscriberBuilder().with_config(self.config)
        common_config = self.config.get_interface_by_name("broker")["common"]

        expected = {
            "host": common_config["host"],
            "port": common_config["port"],
            "user": common_config["user"],
            "password": common_config["password"],
        }
        self.assertEqual(expected, builder.kwargs)

    def test_build(self):
        common_config = self.config.get_interface_by_name("broker")["common"]
        builder = RabbitMQBrokerSubscriberBuilder().with_config(self.config).with_topics({"one", "two"})
        subscriber = builder.build()

        self.assertIsInstance(subscriber, RabbitMQBrokerSubscriber)
        self.assertEqual({"one", "two"}, subscriber.topics)
        self.assertEqual(common_config["host"], subscriber.host)
        self.assertEqual(common_config["port"], subscriber.port)


if __name__ == "__main__":
    unittest.main()
