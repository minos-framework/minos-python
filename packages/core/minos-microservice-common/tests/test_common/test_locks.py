import unittest

from minos.common import (
    Lock,
)
from tests.utils import (
    FakeLock,
)


class TestLock(unittest.TestCase):
    def test_abstract(self):
        with self.assertRaises(TypeError):
            Lock("foo")

    def test_key(self):
        lock = FakeLock("foo")
        self.assertEqual("foo", lock.key)

    def test_key_raises(self):
        with self.assertRaises(ValueError):
            FakeLock([])

    def test_hashed_key(self):
        lock = FakeLock("foo")
        self.assertEqual(hash("foo"), lock.hashed_key)


if __name__ == "__main__":
    unittest.main()
