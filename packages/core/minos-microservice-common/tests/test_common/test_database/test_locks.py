import unittest

import aiopg
from aiopg import (
    Cursor,
)

from minos.common import (
    DatabaseLock,
    Lock,
)
from minos.common.testing import (
    PostgresAsyncTestCase,
)
from tests.utils import (
    CommonTestCase,
)


class TestPostgreSqlLock(CommonTestCase, PostgresAsyncTestCase):
    def test_base(self):
        self.assertTrue(issubclass(DatabaseLock, Lock))

    async def test_wrapped_connection(self):
        wrapped_connection = aiopg.connect(**self.repository_db)
        lock = DatabaseLock(wrapped_connection, "foo")
        self.assertEqual(wrapped_connection, lock.wrapped_connection)

    async def test_key(self):
        wrapped_connection = aiopg.connect(**self.repository_db)
        lock = DatabaseLock(wrapped_connection, "foo")
        self.assertEqual("foo", lock.key)

    async def test_key_raises(self):
        wrapped_connection = aiopg.connect(**self.repository_db)
        with self.assertRaises(ValueError):
            DatabaseLock(wrapped_connection, [])

    async def test_hashed_key(self):
        wrapped_connection = aiopg.connect(**self.repository_db)
        lock = DatabaseLock(wrapped_connection, "foo")
        self.assertEqual(hash("foo"), lock.hashed_key)

    async def test_cursor(self):
        wrapped_connection = aiopg.connect(**self.repository_db)
        async with DatabaseLock(wrapped_connection, "foo") as lock:
            self.assertIsInstance(lock.cursor, Cursor)


if __name__ == "__main__":
    unittest.main()
