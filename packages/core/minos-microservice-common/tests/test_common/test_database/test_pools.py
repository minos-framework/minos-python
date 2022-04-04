import unittest
from unittest.mock import (
    PropertyMock,
    patch,
)

import aiopg
from aiopg import (
    Connection,
)
from psycopg2 import (
    OperationalError,
)

from minos.common import (
    DatabaseClientPool,
    DatabaseLock,
    DatabaseLockPool,
)
from minos.common.testing import (
    PostgresAsyncTestCase,
)
from tests.utils import (
    CommonTestCase,
)


class TestPostgreSqlPool(CommonTestCase, PostgresAsyncTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.pool = DatabaseClientPool.from_config(self.config)

    def test_constructor(self):
        pool = DatabaseClientPool("foo")
        self.assertEqual("foo", pool.database)
        self.assertEqual("postgres", pool.user)
        self.assertEqual("", pool.password)
        self.assertEqual("localhost", pool.host)
        self.assertEqual(5432, pool.port)

    async def asyncSetUp(self):
        await super().asyncSetUp()
        await self.pool.setup()

    async def asyncTearDown(self):
        await self.pool.destroy()
        await super().asyncTearDown()

    def test_from_config(self):
        repository_config = self.config.get_database_by_name("event")
        self.assertEqual(repository_config["database"], self.pool.database)
        self.assertEqual(repository_config["user"], self.pool.user)
        self.assertEqual(repository_config["password"], self.pool.password)
        self.assertEqual(repository_config["host"], self.pool.host)
        self.assertEqual(repository_config["port"], self.pool.port)

    async def test_acquire(self):
        async with self.pool.acquire() as c1:
            self.assertIsInstance(c1, Connection)
        async with self.pool.acquire() as c2:
            self.assertEqual(c1, c2)

    async def test_acquire_with_error(self):
        with patch("aiopg.Connection.isolation_level", new_callable=PropertyMock, side_effect=(OperationalError, None)):
            async with self.pool.acquire() as connection:
                self.assertIsInstance(connection, Connection)

    async def test_acquire_with_connection_error(self):
        executed = [False]
        original = aiopg.connect

        def _side_effect(*args, **kwargs):
            if not executed[0]:
                executed[0] = True
                raise OperationalError
            return original(*args, **kwargs)

        with patch("aiopg.connect", side_effect=_side_effect):
            async with self.pool.acquire() as connection:
                self.assertIsInstance(connection, Connection)


class TestPostgreSqlLockPool(CommonTestCase, PostgresAsyncTestCase):
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
