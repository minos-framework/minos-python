import unittest

from kafka import (
    KafkaAdminClient,
)

from minos.common import (
    NotProvidedException,
)
from minos.common.testing import (
    PostgresAsyncTestCase,
)
from minos.networks import (
    REQUEST_REPLY_TOPIC_CONTEXT_VAR,
    Broker,
    BrokerPool,
    KafkaBrokerPublisher,
)
from tests.utils import (
    BASE_PATH,
)


@unittest.skip("FIXME!")
class TestDynamicBrokerPool(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def setUp(self) -> None:
        super().setUp()
        self.publisher = KafkaBrokerPublisher.from_config(self.config)
        self.consumer = BrokerConsumer.from_config(self.config)  # noqa
        self.pool = BrokerPool.from_config(self.config, consumer=self.consumer, publisher=self.publisher)

    async def asyncSetUp(self):
        await super().asyncSetUp()
        await self.consumer.setup()
        await self.publisher.setup()
        await self.pool.setup()

    async def asyncTearDown(self):
        await self.pool.destroy()
        await self.publisher.destroy()
        await self.consumer.destroy()
        await super().asyncTearDown()

    async def test_config(self):
        self.assertEqual(self.config, self.pool.config)

    async def test_from_config_raises(self):
        with self.assertRaises(NotProvidedException):
            BrokerPool.from_config(self.config)
        with self.assertRaises(NotProvidedException):
            BrokerPool.from_config(self.config, consumer=self.consumer)

    async def test_setup_destroy(self):
        pool = BrokerPool.from_config(self.config, consumer=self.consumer, publisher=self.publisher)
        self.assertTrue(pool.already_setup)
        async with pool:
            self.assertTrue(pool.already_setup)
        self.assertTrue(pool.already_destroyed)

    async def test_client(self):
        self.assertIsInstance(self.pool.client, KafkaAdminClient)
        expected = f"{self.config.broker.host}:{self.config.broker.port}"
        self.assertEqual(expected, self.pool.client.config["bootstrap_servers"])

    async def test_acquire(self):
        async with self.pool.acquire() as broker:
            self.assertIsInstance(broker, Broker)
            self.assertIn(broker.topic, self.pool.client.list_topics())

    async def test_acquire_reply_topic_context_var(self):
        self.assertEqual(None, REQUEST_REPLY_TOPIC_CONTEXT_VAR.get())

        async with self.pool.acquire() as broker:
            self.assertEqual(broker.topic, REQUEST_REPLY_TOPIC_CONTEXT_VAR.get())

        self.assertEqual(None, REQUEST_REPLY_TOPIC_CONTEXT_VAR.get())


if __name__ == "__main__":
    unittest.main()
