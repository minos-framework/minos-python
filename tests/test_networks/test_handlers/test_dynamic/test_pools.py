"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import unittest

from kafka import (
    KafkaAdminClient,
)

from minos.common.testing import (
    PostgresAsyncTestCase,
)
from minos.networks import (
    DynamicReplyHandler,
    ReplyHandlerPool,
)
from tests.utils import (
    BASE_PATH,
)


class TestDynamicHandler(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def setUp(self) -> None:
        super().setUp()
        self.pool = ReplyHandlerPool.from_config(config=self.config)

    async def test_config(self):
        self.assertEqual(self.config, self.pool.config)

    async def test_setup_destroy(self):
        self.assertFalse(self.pool.already_setup)
        async with self.pool:
            self.assertTrue(self.pool.already_setup)
            async with self.pool.acquire():
                pass
        self.assertTrue(self.pool.already_destroyed)

    async def test_client(self):
        client = self.pool.client
        self.assertIsInstance(client, KafkaAdminClient)
        expected = f"{self.config.commands.broker.host}:{self.config.commands.broker.port}"
        self.assertEqual(expected, client.config["bootstrap_servers"])

    async def test_acquire(self):
        client = self.pool.client
        async with self.pool.acquire() as handler:
            self.assertIsInstance(handler, DynamicReplyHandler)
            topic = f"{handler.topic}Reply"
            self.assertIn(topic, client.list_topics())


if __name__ == "__main__":
    unittest.main()
