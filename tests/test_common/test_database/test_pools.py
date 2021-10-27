import unittest
from unittest.mock import (
    PropertyMock,
    patch,
)

from aiopg import (
    Connection,
)
from psycopg2 import (
    OperationalError,
)

from minos.common import (
    PostgreSqlLock,
    PostgreSqlLockPool,
    PostgreSqlPool,
)
from minos.common.testing import (
    PostgresAsyncTestCase,
)
from tests.utils import (
    BASE_PATH,
)


class TestPostgreSqlPool(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def setUp(self) -> None:
        super().setUp()
        self.pool = PostgreSqlPool.from_config(self.config)

    async def asyncSetUp(self):
        await super().asyncSetUp()
        await self.pool.setup()

    async def asyncTearDown(self):
        await self.pool.destroy()
        await super().asyncTearDown()

    def test_from_config(self):
        self.assertEqual(self.config.repository.database, self.pool.database)
        self.assertEqual(self.config.repository.user, self.pool.user)
        self.assertEqual(self.config.repository.password, self.pool.password)
        self.assertEqual(self.config.repository.host, self.pool.host)
        self.assertEqual(self.config.repository.port, self.pool.port)

    async def test_acquire(self):
        async with self.pool.acquire() as connection:
            self.assertIsInstance(connection, Connection)

    async def test_acquire_with_error(self):
        with patch("aiopg.Connection.isolation_level", new_callable=PropertyMock, side_effect=(OperationalError, None)):
            async with self.pool.acquire() as connection:
                self.assertIsInstance(connection, Connection)


class TestPostgreSqlLockPool(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def setUp(self) -> None:
        super().setUp()
        self.pool = PostgreSqlLockPool.from_config(self.config)

    async def asyncSetUp(self):
        await super().asyncSetUp()
        await self.pool.setup()

    async def asyncTearDown(self):
        await self.pool.destroy()
        await super().asyncTearDown()

    async def test_acquire(self):
        async with self.pool.acquire("foo") as lock:
            self.assertIsInstance(lock, PostgreSqlLock)
            self.assertEqual("foo", lock.key)


if __name__ == "__main__":
    unittest.main()
