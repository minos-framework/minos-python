import unittest
from unittest.mock import (
    patch,
)

from minos.common import (
    Config,
    ConnectionException,
    DatabaseClient,
    DatabaseClientBuilder,
    DatabaseClientPool,
    DatabaseLock,
    DatabaseLockPool,
    classname,
)
from minos.common.testing import (
    DatabaseMinosTestCase,
    MockedDatabaseClient,
)
from tests.utils import (
    CONFIG_FILE_PATH,
    CommonTestCase,
)


class TestDatabaseClientPool(CommonTestCase, DatabaseMinosTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.pool = DatabaseClientPool.from_config(self.config)

    async def test_constructor(self):
        builder = DatabaseClientBuilder()
        async with DatabaseClientPool(builder) as pool:
            self.assertEqual(builder, pool.client_builder)

    async def asyncSetUp(self):
        await super().asyncSetUp()
        await self.pool.setup()

    async def asyncTearDown(self):
        await self.pool.destroy()
        await super().asyncTearDown()

    async def test_from_config(self):
        async with DatabaseClientPool.from_config(self.config, key="event") as pool:
            self.assertIsInstance(pool.client_builder, DatabaseClientBuilder)
            self.assertEqual(MockedDatabaseClient, pool.client_builder.instance_cls)

    async def test_from_config_client_builder(self):
        config = Config(CONFIG_FILE_PATH, databases_default_client=classname(DatabaseClientBuilder))
        async with DatabaseClientPool.from_config(config) as pool:
            self.assertIsInstance(pool.client_builder, DatabaseClientBuilder)

    def test_from_config_client_none(self):
        config = Config(CONFIG_FILE_PATH, databases_default_client=None)
        with self.assertRaises(ValueError):
            DatabaseClientPool.from_config(config)

    async def test_acquire_once(self):
        async with self.pool.acquire() as c1:
            self.assertIsInstance(c1, DatabaseClient)

    async def test_acquire_multiple_recycle(self):
        async with self.pool.acquire() as c1:
            pass
        async with self.pool.acquire() as c2:
            self.assertEqual(c1, c2)

    async def test_acquire_multiple_same_time(self):
        async with self.pool.acquire() as c1:
            async with self.pool.acquire() as c2:
                self.assertNotEqual(c1, c2)

    async def test_acquire_with_reset(self):
        with patch.object(MockedDatabaseClient, "reset") as reset_mock:
            async with self.pool.acquire():
                self.assertEqual(0, reset_mock.call_count)
        self.assertEqual(1, reset_mock.call_count)

    async def test_acquire_with_raises(self):
        with patch.object(MockedDatabaseClient, "setup", side_effect=[ConnectionException(""), None]):
            async with self.pool.acquire() as client:
                self.assertIsInstance(client, MockedDatabaseClient)


class TestDatabaseLockPool(CommonTestCase, DatabaseMinosTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.pool = DatabaseLockPool.from_config(self.config)

    async def asyncSetUp(self):
        await super().asyncSetUp()
        await self.pool.setup()

    async def asyncTearDown(self):
        await self.pool.destroy()
        await super().asyncTearDown()

    async def test_acquire(self):
        async with self.pool.acquire("foo") as lock:
            self.assertIsInstance(lock, DatabaseLock)
            self.assertEqual("foo", lock.key)


if __name__ == "__main__":
    unittest.main()
