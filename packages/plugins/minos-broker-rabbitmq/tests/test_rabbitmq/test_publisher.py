import unittest
from unittest.mock import (
    AsyncMock, patch, MagicMock,
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

    async def test_send(self):
        message = BrokerMessageV1("foo", BrokerMessageV1Payload("bar"))

        with patch('aio_pika.connect', return_value=AsyncMock()) as connection_mock:
            async with RabbitMQBrokerPublisher.from_config(CONFIG_FILE_PATH) as publisher:
                await publisher.send(message)

            self.assertEqual(1, connection_mock.call_count)
            self.assertEqual("foo", connection_mock.call_args.args[0])
            self.assertEqual(message, BrokerMessage.from_avro_bytes(connection_mock.call_args.args[1]))

    async def test_setup_destroy(self):
        publisher = RabbitMQBrokerPublisher.from_config(CONFIG_FILE_PATH)
        start_mock = AsyncMock()
        stop_mock = AsyncMock()
        publisher.client.start = start_mock
        publisher.client.stop = stop_mock

        async with publisher:
            self.assertEqual(1, start_mock.call_count)
            self.assertEqual(0, stop_mock.call_count)

            start_mock.reset_mock()
            stop_mock.reset_mock()

        self.assertEqual(0, start_mock.call_count)
        self.assertEqual(1, stop_mock.call_count)


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
