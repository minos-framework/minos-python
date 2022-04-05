import unittest
import warnings
from unittest.mock import (
    PropertyMock,
    patch,
)

import aiopg
from psycopg2 import (
    OperationalError,
)

from minos.common import (
    AiopgDatabaseClient,
    DatabaseClient,
    DatabaseClientBuilder,
    DatabaseClientPool,
    DatabaseLock,
    DatabaseLockPool,
    PostgreSqlLockPool,
    PostgreSqlPool,
)
from minos.common.testing import (
    PostgresAsyncTestCase,
)
from tests.utils import (
    CommonTestCase,
)


class TestDatabaseClientPool(CommonTestCase, PostgresAsyncTestCase):
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
        self.assertEqual(AiopgDatabaseClient, pool.client_builder.instance_cls)

    async def test_acquire(self):
        async with self.pool.acquire() as c1:
            self.assertIsInstance(c1, DatabaseClient)
        async with self.pool.acquire() as c2:
            self.assertEqual(c1, c2)

    async def test_acquire_with_error(self):
        with patch("aiopg.Connection.isolation_level", new_callable=PropertyMock, side_effect=(OperationalError, None)):
            async with self.pool.acquire() as client:
                self.assertIsInstance(client, DatabaseClient)

    async def test_acquire_with_connection_error(self):
        executed = [False]
        original = aiopg.connect

        def _side_effect(*args, **kwargs):
            if not executed[0]:
                executed[0] = True
                raise OperationalError
            return original(*args, **kwargs)

        with patch("aiopg.connect", side_effect=_side_effect):
            async with self.pool.acquire() as client:
                self.assertIsInstance(client, DatabaseClient)


class TestPostgreSqlPool(CommonTestCase, PostgresAsyncTestCase):
    def test_is_subclass(self):
        self.assertTrue(issubclass(PostgreSqlPool, DatabaseClientPool))

    def test_warnings(self):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            pool = PostgreSqlPool.from_config(self.config)
            self.assertIsInstance(pool, DatabaseClientPool)


class TestDatabaseLockPool(CommonTestCase, PostgresAsyncTestCase):
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


class TestPostgreSqlLockPool(CommonTestCase, PostgresAsyncTestCase):
    def test_is_subclass(self):
        self.assertTrue(issubclass(PostgreSqlLockPool, DatabaseLockPool))

    def test_warnings(self):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            pool = PostgreSqlLockPool.from_config(self.config)
            self.assertIsInstance(pool, DatabaseLockPool)


if __name__ == "__main__":
    unittest.main()
