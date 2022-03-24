import unittest
import warnings
from unittest.mock import (
    AsyncMock,
)

from aiokafka import (
    AIOKafkaProducer,
)
from kafka.errors import (
    KafkaConnectionError,
)

from minos.common import (
    Config,
)
from minos.networks import (
    BrokerMessage,
    BrokerMessageV1,
    BrokerMessageV1Payload,
    BrokerPublisher,
    InMemoryBrokerPublisherQueue,
    PostgreSqlBrokerPublisherQueue,
)
from minos.plugins.kafka import (
    InMemoryQueuedKafkaBrokerPublisher,
    KafkaBrokerPublisher,
    KafkaBrokerPublisherBuilder,
    PostgreSqlQueuedKafkaBrokerPublisher,
)
from tests.utils import (
    CONFIG_FILE_PATH,
)


class TestKafkaBrokerPublisher(unittest.IsolatedAsyncioTestCase):
    def test_is_subclass(self):
        self.assertTrue(issubclass(KafkaBrokerPublisher, BrokerPublisher))

    def test_constructor(self):
        publisher = KafkaBrokerPublisher()
        self.assertEqual("localhost", publisher.host)
        self.assertEqual(9092, publisher.port)

    def test_from_config(self):
        config = Config(CONFIG_FILE_PATH)
        broker_config = config.get_interface_by_name("broker")["common"]

        publisher = KafkaBrokerPublisher.from_config(config)

        self.assertIsInstance(publisher, KafkaBrokerPublisher)
        self.assertEqual(broker_config["host"], publisher.host)
        self.assertEqual(broker_config["port"], publisher.port)

    async def test_client(self):
        publisher = KafkaBrokerPublisher.from_config(CONFIG_FILE_PATH)

        self.assertIsInstance(publisher.client, AIOKafkaProducer)

    async def test_start_without_connection(self):
        publisher = KafkaBrokerPublisher.from_config(CONFIG_FILE_PATH, circuit_breaker_time=0.1)
        stop_mock = AsyncMock(side_effect=publisher.client.stop)

        async def _fn():
            if publisher.is_circuit_breaker_recovering:
                raise ValueError()
            raise KafkaConnectionError()

        start_mock = AsyncMock(side_effect=_fn)
        publisher.client.start = start_mock
        publisher.client.stop = stop_mock

        with self.assertRaises(ValueError):
            async with publisher:
                pass

        self.assertEqual(1, stop_mock.call_count)

    async def test_send(self):
        send_mock = AsyncMock()
        message = BrokerMessageV1("foo", BrokerMessageV1Payload("bar"))

        async with KafkaBrokerPublisher.from_config(CONFIG_FILE_PATH) as publisher:
            publisher.client.send_and_wait = send_mock
            await publisher.send(message)

        self.assertEqual(1, send_mock.call_count)
        self.assertEqual("foo", send_mock.call_args.args[0])
        self.assertEqual(message, BrokerMessage.from_avro_bytes(send_mock.call_args.args[1]))

    async def test_send_without_connection(self):
        async def _fn(*args, **kwargs):
            if publisher.is_circuit_breaker_recovering:
                raise ValueError()
            raise KafkaConnectionError()

        mock = AsyncMock(side_effect=_fn)

        message = BrokerMessageV1("foo", BrokerMessageV1Payload("bar"))

        async with KafkaBrokerPublisher.from_config(CONFIG_FILE_PATH, circuit_breaker_time=0.1) as publisher:
            stop_mock = AsyncMock(side_effect=publisher.client.stop)
            publisher.client.stop = stop_mock
            publisher.client.send_and_wait = mock

            with self.assertRaises(ValueError):
                await publisher.send(message)

            self.assertEqual(0, stop_mock.call_count)

    async def test_setup_destroy(self):
        publisher = KafkaBrokerPublisher.from_config(CONFIG_FILE_PATH)
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


class TestKafkaBrokerPublisherBuilder(unittest.TestCase):
    def setUp(self) -> None:
        self.config = Config(CONFIG_FILE_PATH)

    def test_with_config(self):
        builder = KafkaBrokerPublisherBuilder().with_config(self.config)
        common_config = self.config.get_interface_by_name("broker")["common"]

        expected = {
            "group_id": self.config.get_name(),
            "broker_host": common_config["host"],
            "broker_port": common_config["port"],
        }
        self.assertEqual(expected, builder.kwargs)

    def test_build(self):
        common_config = self.config.get_interface_by_name("broker")["common"]
        builder = KafkaBrokerPublisherBuilder().with_config(self.config)
        publisher = builder.build()

        self.assertIsInstance(publisher, KafkaBrokerPublisher)
        self.assertEqual(common_config["host"], publisher.broker_host)
        self.assertEqual(common_config["port"], publisher.broker_port)


class TestPostgreSqlQueuedKafkaBrokerPublisher(unittest.IsolatedAsyncioTestCase):
    def test_from_config(self):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            publisher = PostgreSqlQueuedKafkaBrokerPublisher.from_config(CONFIG_FILE_PATH)
        self.assertIsInstance(publisher, PostgreSqlQueuedKafkaBrokerPublisher)
        self.assertIsInstance(publisher.impl, KafkaBrokerPublisher)
        self.assertIsInstance(publisher.queue, PostgreSqlBrokerPublisherQueue)


class TestInMemoryQueuedKafkaBrokerPublisher(unittest.IsolatedAsyncioTestCase):
    def test_from_config(self):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            publisher = InMemoryQueuedKafkaBrokerPublisher.from_config(CONFIG_FILE_PATH)
        self.assertIsInstance(publisher, InMemoryQueuedKafkaBrokerPublisher)
        self.assertIsInstance(publisher.impl, KafkaBrokerPublisher)
        self.assertIsInstance(publisher.queue, InMemoryBrokerPublisherQueue)


if __name__ == "__main__":
    unittest.main()
