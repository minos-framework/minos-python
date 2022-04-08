import unittest
from unittest.mock import (
    patch,
)

from minos.common import (
    Config,
    DatabaseClient,
    DatabaseClientBuilder,
    DatabaseClientPool,
    DatabaseLock,
    DatabaseLockPool,
    classname,
)
from minos.common.testing import (
    DatabaseMinosTestCase,
)
from tests.utils import (
    CONFIG_FILE_PATH,
    CommonTestCase,
    FakeDatabaseClient,
)


class TestDatabaseClientPool(CommonTestCase, DatabaseMinosTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.pool = DatabaseClientPool.from_config(self.config)

    def test_constructor(self):
        builder = DatabaseClientBuilder()
        pool = DatabaseClientPool(builder)
        self.assertEqual(builder, pool.client_builder)

    async def asyncSetUp(self):
        await super().asyncSetUp()
        await self.pool.setup()

    async def asyncTearDown(self):
        await self.pool.destroy()
        await super().asyncTearDown()

    def test_from_config(self):
        pool = DatabaseClientPool.from_config(self.config, key="event")
        self.assertIsInstance(pool.client_builder, DatabaseClientBuilder)
        self.assertEqual(FakeDatabaseClient, pool.client_builder.instance_cls)

    def test_from_config_client_builder(self):
        config = Config(CONFIG_FILE_PATH, databases_default_client=classname(DatabaseClientBuilder))
        pool = DatabaseClientPool.from_config(config)
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
        with patch.object(FakeDatabaseClient, "reset") as reset_mock:
            async with self.pool.acquire():
                self.assertEqual(0, reset_mock.call_count)
        self.assertEqual(1, reset_mock.call_count)

    async def test_acquire_with_connection_error(self):
        with patch.object(FakeDatabaseClient, "is_valid", return_value=True):
            async with self.pool.acquire() as client:
                self.assertIsInstance(client, FakeDatabaseClient)


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
