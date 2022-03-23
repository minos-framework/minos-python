import unittest
from collections import (
    namedtuple,
)
from unittest.mock import (
    AsyncMock,
    MagicMock,
    patch,
)

from minos.common import (
    MinosConfig, Config,
)
from minos.networks import (
    BrokerMessageV1,
    BrokerMessageV1Payload,
    BrokerSubscriber,
    InMemoryBrokerSubscriberQueue,
    PostgreSqlBrokerSubscriberQueue,
    QueuedBrokerSubscriber,
)
from minos.plugins.rabbitmq import (
    InMemoryQueuedRabbitMQBrokerSubscriberBuilder,
    PostgreSqlQueuedRabbitMQBrokerSubscriberBuilder,
    RabbitMQBrokerSubscriber,
    RabbitMQBrokerSubscriberBuilder,
)
from tests.utils import (
    CONFIG_FILE_PATH,
)

_ConsumerMessage = namedtuple("_ConsumerMessage", ["value"])


class TestRabbitMQBrokerSubscriber(unittest.IsolatedAsyncioTestCase):
    def test_is_subclass(self):
        self.assertTrue(issubclass(RabbitMQBrokerSubscriber, BrokerSubscriber))

    async def test_from_config(self):
        config = Config(CONFIG_FILE_PATH)
        broker_config = config.get_interface_by_name("broker")["common"]
        async with RabbitMQBrokerSubscriber.from_config(config, topics={"foo", "bar"}) as subscriber:
            self.assertEqual(broker_config["host"], subscriber.broker_host)
            self.assertEqual(broker_config["port"], subscriber.broker_port)
            self.assertEqual(config.get_name(), subscriber.group_id)
            self.assertEqual(False, subscriber.remove_topics_on_destroy)
            self.assertEqual({"foo", "bar"}, subscriber.topics)

    @patch("minos.plugins.rabbitmq.subscriber.connect")
    @patch("minos.networks.BrokerMessage.from_avro_bytes")
    async def test_receive(self, connect_mock, mock_avro):
        messages = [
            BrokerMessageV1("foo", BrokerMessageV1Payload("bar")),
            BrokerMessageV1("bar", BrokerMessageV1Payload("foo")),
        ]

        async with RabbitMQBrokerSubscriber.from_config(CONFIG_FILE_PATH, topics={"foo", "bar"}) as subscriber:
            await subscriber.receive()

            self.assertEqual(1, connect_mock.call_count)


class TestRabbitMQBrokerSubscriberBuilder(unittest.TestCase):
    def setUp(self) -> None:
        self.config = Config(CONFIG_FILE_PATH)

    def test_with_config(self):
        builder = RabbitMQBrokerSubscriberBuilder().with_config(self.config)
        common_config = self.config.get_interface_by_name("broker")["common"]

        expected = {
            "group_id": self.config.get_name(),
            "broker_host": common_config["host"],
            "broker_port": common_config["port"],
        }
        self.assertEqual(expected, builder.kwargs)

    def test_build(self):
        common_config = self.config.get_interface_by_name("broker")["common"]
        builder = RabbitMQBrokerSubscriberBuilder().with_config(self.config).with_topics({"one", "two"})
        subscriber = builder.build()

        self.assertIsInstance(subscriber, RabbitMQBrokerSubscriber)
        self.assertEqual({"one", "two"}, subscriber.topics)
        self.assertEqual(common_config["host"], subscriber.broker_host)
        self.assertEqual(common_config["port"], subscriber.broker_port)


class TestPostgreSqlQueuedRabbitMQBrokerSubscriberBuilder(unittest.TestCase):
    def setUp(self) -> None:
        self.config = Config(CONFIG_FILE_PATH)

    def test_build(self):
        builder = PostgreSqlQueuedRabbitMQBrokerSubscriberBuilder().with_config(self.config).with_topics({"one", "two"})
        subscriber = builder.build()

        self.assertIsInstance(subscriber, QueuedBrokerSubscriber)
        self.assertIsInstance(subscriber.impl, RabbitMQBrokerSubscriber)
        self.assertIsInstance(subscriber.queue, PostgreSqlBrokerSubscriberQueue)


class TestInMemoryQueuedRabbitMQBrokerSubscriberBuilder(unittest.TestCase):
    def setUp(self) -> None:
        self.config = Config(CONFIG_FILE_PATH)

    def test_build(self):
        builder = InMemoryQueuedRabbitMQBrokerSubscriberBuilder().with_config(self.config).with_topics({"one", "two"})
        subscriber = builder.build()

        self.assertIsInstance(subscriber, QueuedBrokerSubscriber)
        self.assertIsInstance(subscriber.impl, RabbitMQBrokerSubscriber)
        self.assertIsInstance(subscriber.queue, InMemoryBrokerSubscriberQueue)


if __name__ == "__main__":
    unittest.main()
