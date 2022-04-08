import unittest

from minos.common import (
    DatabaseLock,
    Lock,
)
from tests.utils import (
    FakeDatabaseClient,
)


class TestDatabaseLock(unittest.IsolatedAsyncioTestCase):
    def test_base(self):
        self.assertTrue(issubclass(DatabaseLock, Lock))

    async def test_client(self):
        client = FakeDatabaseClient()
        lock = DatabaseLock(client, "foo")
        self.assertEqual(client, lock.client)

    async def test_key(self):
        client = FakeDatabaseClient()
        lock = DatabaseLock(client, "foo")
        self.assertEqual("foo", lock.key)

    async def test_key_raises(self):
        client = FakeDatabaseClient()
        with self.assertRaises(ValueError):
            DatabaseLock(client, [])

    async def test_hashed_key(self):
        client = FakeDatabaseClient()
        lock = DatabaseLock(client, "foo")
        self.assertEqual(hash("foo"), lock.hashed_key)


if __name__ == "__main__":
    unittest.main()
