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
    REPLY_TOPIC_CONTEXT_VAR,
    BrokerConsumer,
    DynamicBrokerHandler,
    DynamicBrokerHandlerPool,
)
from tests.utils import (
    BASE_PATH,
)


class TestDynamicHandlerPool(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def setUp(self) -> None:
        super().setUp()
        self.consumer = BrokerConsumer.from_config(config=self.config)
        self.pool = DynamicBrokerHandlerPool.from_config(config=self.config, consumer=self.consumer)

    async def test_config(self):
        self.assertEqual(self.config, self.pool.config)

    async def test_from_config_raises(self):
        with self.assertRaises(NotProvidedException):
            DynamicBrokerHandlerPool.from_config(config=self.config)

    async def test_setup_destroy(self):
        self.assertTrue(self.pool.already_setup)
        async with self.consumer, self.pool:
            self.assertTrue(self.pool.already_setup)
        self.assertTrue(self.pool.already_destroyed)

    async def test_client(self):
        client = self.pool.client
        self.assertIsInstance(client, KafkaAdminClient)
        expected = f"{self.config.broker.host}:{self.config.broker.port}"
        self.assertEqual(expected, client.config["bootstrap_servers"])

    async def test_acquire(self):
        async with self.consumer, self.pool:
            async with self.pool.acquire() as handler:
                self.assertIsInstance(handler, DynamicBrokerHandler)
                self.assertIn(handler.topic, self.pool.client.list_topics())

    async def test_acquire_reply_topic_context_var(self):
        self.assertEqual(None, REPLY_TOPIC_CONTEXT_VAR.get())

        async with self.consumer, self.pool:
            async with self.pool.acquire() as handler:
                self.assertEqual(handler.topic, REPLY_TOPIC_CONTEXT_VAR.get())

        self.assertEqual(None, REPLY_TOPIC_CONTEXT_VAR.get())


if __name__ == "__main__":
    unittest.main()
