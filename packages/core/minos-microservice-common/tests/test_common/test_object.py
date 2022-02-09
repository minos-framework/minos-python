import unittest

from minos.common import (
    Object,
)


class TestObject(unittest.TestCase):
    def test_empty(self):
        obj = Object()
        self.assertIsInstance(obj, Object)

    def test_args_kwargs(self):
        obj = Object("foo", bar="foobar")
        self.assertIsInstance(obj, Object)

    def test_comparisons(self):
        self.assertNotEqual(Object(), Object())


if __name__ == "__main__":
    unittest.main()
