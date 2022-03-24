import unittest
from collections import (
    namedtuple,
)
from unittest.mock import (
    AsyncMock,
    MagicMock,
)

from aiokafka import (
    AIOKafkaConsumer,
)
from kafka import (
    KafkaAdminClient,
)
from kafka.errors import (
    KafkaConnectionError,
)

from minos.common import (
    Config,
)
from minos.networks import (
    BrokerMessageV1,
    BrokerMessageV1Payload,
    BrokerSubscriber,
    InMemoryBrokerSubscriberQueue,
    PostgreSqlBrokerSubscriberQueue,
    QueuedBrokerSubscriber,
)
from minos.plugins.kafka import (
    InMemoryQueuedKafkaBrokerSubscriberBuilder,
    KafkaBrokerSubscriber,
    KafkaBrokerSubscriberBuilder,
    PostgreSqlQueuedKafkaBrokerSubscriberBuilder,
)
from tests.utils import (
    CONFIG_FILE_PATH,
)

_ConsumerMessage = namedtuple("_ConsumerMessage", ["value"])


class TestKafkaBrokerSubscriber(unittest.IsolatedAsyncioTestCase):
    def test_is_subclass(self):
        self.assertTrue(issubclass(KafkaBrokerSubscriber, BrokerSubscriber))

    async def test_from_config(self):
        config = Config(CONFIG_FILE_PATH)
        broker_config = config.get_interface_by_name("broker")["common"]
        async with KafkaBrokerSubscriber.from_config(config, topics={"foo", "bar"}) as subscriber:
            self.assertEqual(broker_config["host"], subscriber.broker_host)
            self.assertEqual(broker_config["port"], subscriber.broker_port)
            self.assertEqual(config.get_name(), subscriber.group_id)
            self.assertEqual(False, subscriber.remove_topics_on_destroy)
            self.assertEqual({"foo", "bar"}, subscriber.topics)

    async def test_from_config_none_group_id(self):
        async with KafkaBrokerSubscriber.from_config(
            CONFIG_FILE_PATH, topics={"foo", "bar"}, group_id=None
        ) as subscriber:
            self.assertEqual(None, subscriber.group_id)

    async def test_client(self):
        async with KafkaBrokerSubscriber.from_config(CONFIG_FILE_PATH, topics={"foo", "bar"}) as subscriber:
            client = subscriber.client
            self.assertIsInstance(client, AIOKafkaConsumer)

    async def test_admin_client(self):
        async with KafkaBrokerSubscriber.from_config(CONFIG_FILE_PATH, topics={"foo", "bar"}) as subscriber:
            client = subscriber.admin_client
            self.assertIsInstance(client, KafkaAdminClient)

    async def test_setup_destroy_without_connection(self):
        publisher = KafkaBrokerSubscriber.from_config(CONFIG_FILE_PATH, topics={"foo", "bar"}, circuit_breaker_time=0.1)
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

    async def test_setup_destroy_client(self):
        subscriber = KafkaBrokerSubscriber.from_config(CONFIG_FILE_PATH, topics={"foo", "bar"})

        start_mock = AsyncMock()
        stop_mock = AsyncMock()
        subscriber.client.start = start_mock
        subscriber.client.stop = stop_mock

        admin_stop_mock = MagicMock()
        admin_create_topic_mock = MagicMock()
        admin_remove_topic_mock = MagicMock()
        subscriber.admin_client.stop = admin_stop_mock
        subscriber.admin_client.create_topics = admin_create_topic_mock
        subscriber.admin_client.delete_topics = admin_remove_topic_mock

        async with subscriber:
            self.assertEqual(1, start_mock.call_count)
            self.assertEqual(0, stop_mock.call_count)

            start_mock.reset_mock()
            stop_mock.reset_mock()

        self.assertEqual(0, start_mock.call_count)
        self.assertEqual(1, stop_mock.call_count)

    async def test_setup_destroy_admin_client(self):
        subscriber = KafkaBrokerSubscriber.from_config(CONFIG_FILE_PATH, topics={"foo", "bar"})

        start_mock = AsyncMock()
        stop_mock = AsyncMock()
        subscriber.client.start = start_mock
        subscriber.client.stop = stop_mock

        admin_stop_mock = MagicMock()
        admin_create_topic_mock = MagicMock()
        admin_remove_topic_mock = MagicMock()
        subscriber.admin_client.stop = admin_stop_mock
        subscriber.admin_client.create_topics = admin_create_topic_mock
        subscriber.admin_client.delete_topics = admin_remove_topic_mock

        async with subscriber:
            self.assertEqual(0, admin_stop_mock.call_count)
            self.assertEqual(1, admin_create_topic_mock.call_count)

            observed = admin_create_topic_mock.call_args.args[0]
            self.assertIsInstance(observed, list)
            self.assertEqual(2, len(observed))
            self.assertEqual({"foo", "bar"}, set(o.name for o in observed))

            self.assertEqual(0, admin_remove_topic_mock.call_count)

            admin_stop_mock.reset_mock()
            admin_create_topic_mock.reset_mock()
            admin_remove_topic_mock.reset_mock()

        self.assertEqual(0, admin_stop_mock.call_count)
        self.assertEqual(0, admin_create_topic_mock.call_count)
        self.assertEqual(0, admin_remove_topic_mock.call_count)

    async def test_setup_destroy_admin_client_removing_topics(self):
        subscriber = KafkaBrokerSubscriber.from_config(
            CONFIG_FILE_PATH, topics={"foo", "bar"}, remove_topics_on_destroy=True
        )

        start_mock = AsyncMock()
        stop_mock = AsyncMock()
        subscriber.client.start = start_mock
        subscriber.client.stop = stop_mock

        admin_stop_mock = MagicMock()
        admin_create_topic_mock = MagicMock()
        admin_remove_topic_mock = MagicMock()
        subscriber.admin_client.stop = admin_stop_mock
        subscriber.admin_client.create_topics = admin_create_topic_mock
        subscriber.admin_client.delete_topics = admin_remove_topic_mock

        async with subscriber:
            self.assertEqual(0, admin_stop_mock.call_count)
            self.assertEqual(1, admin_create_topic_mock.call_count)
            self.assertEqual(0, admin_remove_topic_mock.call_count)

            admin_stop_mock.reset_mock()
            admin_create_topic_mock.reset_mock()
            admin_remove_topic_mock.reset_mock()

        self.assertEqual(0, admin_stop_mock.call_count)
        self.assertEqual(0, admin_create_topic_mock.call_count)
        self.assertEqual(1, admin_remove_topic_mock.call_count)

        observed = admin_remove_topic_mock.call_args.args[0]
        self.assertIsInstance(observed, list)
        self.assertEqual(2, len(observed))

        self.assertEqual({"foo", "bar"}, set(observed))

    async def test_receive(self):
        messages = [
            BrokerMessageV1("foo", BrokerMessageV1Payload("bar")),
            BrokerMessageV1("bar", BrokerMessageV1Payload("foo")),
        ]

        async with KafkaBrokerSubscriber.from_config(CONFIG_FILE_PATH, topics={"foo", "bar"}) as subscriber:
            get_mock = AsyncMock(side_effect=[_ConsumerMessage(m.avro_bytes) for m in messages])
            subscriber.client.getone = get_mock

            self.assertEqual(messages[0], await subscriber.receive())
            self.assertEqual(messages[1], await subscriber.receive())


class TestKafkaBrokerSubscriberBuilder(unittest.TestCase):
    def setUp(self) -> None:
        self.config = Config(CONFIG_FILE_PATH)

    def test_with_config(self):
        builder = KafkaBrokerSubscriberBuilder().with_config(self.config)
        common_config = self.config.get_interface_by_name("broker")["common"]

        expected = {
            "group_id": self.config.get_name(),
            "broker_host": common_config["host"],
            "broker_port": common_config["port"],
        }
        self.assertEqual(expected, builder.kwargs)

    def test_build(self):
        common_config = self.config.get_interface_by_name("broker")["common"]
        builder = KafkaBrokerSubscriberBuilder().with_config(self.config).with_topics({"one", "two"})
        subscriber = builder.build()

        self.assertIsInstance(subscriber, KafkaBrokerSubscriber)
        self.assertEqual({"one", "two"}, subscriber.topics)
        self.assertEqual(common_config["host"], subscriber.broker_host)
        self.assertEqual(common_config["port"], subscriber.broker_port)


class TestPostgreSqlQueuedKafkaBrokerSubscriberBuilder(unittest.TestCase):
    def setUp(self) -> None:
        self.config = Config(CONFIG_FILE_PATH)

    def test_build(self):
        builder = PostgreSqlQueuedKafkaBrokerSubscriberBuilder().with_config(self.config).with_topics({"one", "two"})
        subscriber = builder.build()

        self.assertIsInstance(subscriber, QueuedBrokerSubscriber)
        self.assertIsInstance(subscriber.impl, KafkaBrokerSubscriber)
        self.assertIsInstance(subscriber.queue, PostgreSqlBrokerSubscriberQueue)


class TestInMemoryQueuedKafkaBrokerSubscriberBuilder(unittest.TestCase):
    def setUp(self) -> None:
        self.config = Config(CONFIG_FILE_PATH)

    def test_build(self):
        builder = InMemoryQueuedKafkaBrokerSubscriberBuilder().with_config(self.config).with_topics({"one", "two"})
        subscriber = builder.build()

        self.assertIsInstance(subscriber, QueuedBrokerSubscriber)
        self.assertIsInstance(subscriber.impl, KafkaBrokerSubscriber)
        self.assertIsInstance(subscriber.queue, InMemoryBrokerSubscriberQueue)


if __name__ == "__main__":
    unittest.main()
