import unittest
import warnings

import aiopg
from aiopg import (
    Cursor,
)

from minos.common import (
    DatabaseLock,
    Lock,
    PostgreSqlLock,
)
from minos.common.testing import (
    PostgresAsyncTestCase,
)
from tests.utils import (
    CommonTestCase,
)


class TestDatabaseLock(CommonTestCase, PostgresAsyncTestCase):
    def test_base(self):
        self.assertTrue(issubclass(DatabaseLock, Lock))

    async def test_wrapped_connection(self):
        wrapped_connection = aiopg.connect(**self.config.get_default_database())
        lock = DatabaseLock(wrapped_connection, "foo")
        self.assertEqual(wrapped_connection, lock.wrapped_connection)

    async def test_key(self):
        wrapped_connection = aiopg.connect(**self.config.get_default_database())
        lock = DatabaseLock(wrapped_connection, "foo")
        self.assertEqual("foo", lock.key)

    async def test_key_raises(self):
        wrapped_connection = aiopg.connect(**self.config.get_default_database())
        with self.assertRaises(ValueError):
            DatabaseLock(wrapped_connection, [])

    async def test_hashed_key(self):
        wrapped_connection = aiopg.connect(**self.config.get_default_database())
        lock = DatabaseLock(wrapped_connection, "foo")
        self.assertEqual(hash("foo"), lock.hashed_key)

    async def test_cursor(self):
        wrapped_connection = aiopg.connect(**self.config.get_default_database())
        async with DatabaseLock(wrapped_connection, "foo") as lock:
            self.assertIsInstance(lock.cursor, Cursor)


class TestPostgreSqlLock(CommonTestCase, PostgresAsyncTestCase):
    def test_is_subclass(self):
        self.assertTrue(issubclass(PostgreSqlLock, DatabaseLock))

    async def test_warnings(self):
        wrapped_connection = aiopg.connect(**self.config.get_default_database())

        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            lock = PostgreSqlLock(wrapped_connection, "foo")
            self.assertIsInstance(lock, DatabaseLock)


if __name__ == "__main__":
    unittest.main()
