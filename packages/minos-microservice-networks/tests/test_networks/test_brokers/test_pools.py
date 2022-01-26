import unittest

from minos.common.testing import (
    PostgresAsyncTestCase,
)
from minos.networks import (
    REQUEST_REPLY_TOPIC_CONTEXT_VAR,
    BrokerClient,
    BrokerClientPool,
    InMemoryBrokerPublisher,
    InMemoryBrokerSubscriberBuilder,
)
from tests.utils import (
    BASE_PATH,
)


class TestBrokerClientPool(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def setUp(self) -> None:
        super().setUp()
        self.publisher = InMemoryBrokerPublisher.from_config(self.config)
        self.subscriber_builder = InMemoryBrokerSubscriberBuilder()
        self.pool = BrokerClientPool.from_config(
            self.config, publisher=self.publisher, subscriber_builder=self.subscriber_builder
        )

    async def asyncSetUp(self):
        await super().asyncSetUp()
        await self.publisher.setup()
        await self.pool.setup()

    async def asyncTearDown(self):
        await self.pool.destroy()
        await self.publisher.destroy()
        await super().asyncTearDown()

    async def test_setup_destroy(self):
        pool = BrokerClientPool.from_config(self.config, publisher=self.publisher)
        self.assertTrue(pool.already_setup)
        async with pool:
            self.assertTrue(pool.already_setup)
        self.assertTrue(pool.already_destroyed)

    async def test_acquire(self):
        async with self.pool.acquire() as broker:
            self.assertIsInstance(broker, BrokerClient)

    async def test_acquire_reply_topic_context_var(self):
        self.assertEqual(None, REQUEST_REPLY_TOPIC_CONTEXT_VAR.get())

        async with self.pool.acquire() as broker:
            self.assertEqual(broker.topic, REQUEST_REPLY_TOPIC_CONTEXT_VAR.get())

        self.assertEqual(None, REQUEST_REPLY_TOPIC_CONTEXT_VAR.get())


if __name__ == "__main__":
    unittest.main()
