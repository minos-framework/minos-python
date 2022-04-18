import unittest

from minos.common import (
    DatabaseLock,
    Lock,
)
from minos.common.testing import (
    MockedDatabaseClient,
)


class TestDatabaseLock(unittest.IsolatedAsyncioTestCase):
    def test_base(self):
        self.assertTrue(issubclass(DatabaseLock, Lock))

    async def test_client(self):
        client = MockedDatabaseClient()
        lock = DatabaseLock(client, "foo")
        self.assertEqual(client, lock.client)

    async def test_key(self):
        client = MockedDatabaseClient()
        lock = DatabaseLock(client, "foo")
        self.assertEqual("foo", lock.key)

    async def test_key_raises(self):
        client = MockedDatabaseClient()
        with self.assertRaises(ValueError):
            DatabaseLock(client, [])

    async def test_hashed_key(self):
        client = MockedDatabaseClient()
        lock = DatabaseLock(client, "foo")
        self.assertEqual(hash("foo"), lock.hashed_key)


if __name__ == "__main__":
    unittest.main()
