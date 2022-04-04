import unittest
import warnings

from minos.common import (
    AiopgDatabaseClient,
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

    async def test_client(self):
        client = AiopgDatabaseClient(**self.config.get_default_database())
        lock = DatabaseLock(client, "foo")
        self.assertEqual(client, lock.client)

    async def test_key(self):
        client = AiopgDatabaseClient(**self.config.get_default_database())
        lock = DatabaseLock(client, "foo")
        self.assertEqual("foo", lock.key)

    async def test_key_raises(self):
        client = AiopgDatabaseClient(**self.config.get_default_database())
        with self.assertRaises(ValueError):
            DatabaseLock(client, [])

    async def test_hashed_key(self):
        client = AiopgDatabaseClient(**self.config.get_default_database())
        lock = DatabaseLock(client, "foo")
        self.assertEqual(hash("foo"), lock.hashed_key)


class TestPostgreSqlLock(CommonTestCase, PostgresAsyncTestCase):
    def test_is_subclass(self):
        self.assertTrue(issubclass(PostgreSqlLock, DatabaseLock))

    async def test_warnings(self):
        client = AiopgDatabaseClient(**self.config.get_default_database())

        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            lock = PostgreSqlLock(client, "foo")
            self.assertIsInstance(lock, DatabaseLock)


if __name__ == "__main__":
    unittest.main()
