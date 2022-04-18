import unittest

from minos.common import (
    AiopgDatabaseClient,
    DatabaseLock,
    Lock,
)
from minos.common.testing import (
    DatabaseMinosTestCase,
)
from tests.utils import (
    CommonTestCase,
)


class TestDatabaseLock(CommonTestCase, DatabaseMinosTestCase):
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


if __name__ == "__main__":
    unittest.main()
