import unittest
from unittest.mock import (
    AsyncMock,
    patch,
)

import aio_pika

from minos.common import (
    MinosConfig,
)
from minos.networks import (
    BrokerMessage,
    BrokerMessageV1,
    BrokerMessageV1Payload,
    BrokerPublisher,
    InMemoryBrokerPublisherQueue,
    PostgreSqlBrokerPublisherQueue,
)
from minos.plugins.rabbitmq import (
    InMemoryQueuedRabbitMQBrokerPublisher,
    RabbitMQBrokerPublisher,
    PostgreSqlQueuedRabbitMQBrokerPublisher,
)
from tests.utils import (
    CONFIG_FILE_PATH,
)


class TestRabbitMQBrokerPublisher(unittest.IsolatedAsyncioTestCase):
    def test_is_subclass(self):
        self.assertTrue(issubclass(RabbitMQBrokerPublisher, BrokerPublisher))

    def test_from_config(self):
        config = MinosConfig(CONFIG_FILE_PATH)
        publisher = RabbitMQBrokerPublisher.from_config(config)

        self.assertIsInstance(publisher, RabbitMQBrokerPublisher)
        self.assertEqual(config.broker.host, publisher.broker_host)
        self.assertEqual(config.broker.port, publisher.broker_port)

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


class TestPostgreSqlQueuedRabbitMQBrokerPublisher(unittest.IsolatedAsyncioTestCase):
    def test_from_config(self):
        publisher = PostgreSqlQueuedRabbitMQBrokerPublisher.from_config(CONFIG_FILE_PATH)
        self.assertIsInstance(publisher, PostgreSqlQueuedRabbitMQBrokerPublisher)
        self.assertIsInstance(publisher.impl, RabbitMQBrokerPublisher)
        self.assertIsInstance(publisher.queue, PostgreSqlBrokerPublisherQueue)


class TestInMemoryQueuedRabbitMQBrokerPublisher(unittest.IsolatedAsyncioTestCase):
    def test_from_config(self):
        publisher = InMemoryQueuedRabbitMQBrokerPublisher.from_config(CONFIG_FILE_PATH)
        self.assertIsInstance(publisher, InMemoryQueuedRabbitMQBrokerPublisher)
        self.assertIsInstance(publisher.impl, RabbitMQBrokerPublisher)
        self.assertIsInstance(publisher.queue, InMemoryBrokerPublisherQueue)


if __name__ == "__main__":
    unittest.main()
