import unittest

from minos.common import (
    MissingSentinel,
    NoneType,
)


class TestNoneType(unittest.TestCase):
    def test_equal(self):
        self.assertEqual(type(None), NoneType)


class TestMissingSentinel(unittest.TestCase):
    def test_subclass(self):
        self.assertTrue(issubclass(MissingSentinel, object))


if __name__ == "__main__":
    unittest.main()
