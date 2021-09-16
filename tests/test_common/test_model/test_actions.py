import unittest

from minos.common import (
    Action,
    MinosModelException,
)


class TestAction(unittest.TestCase):
    def test_value_of(self):
        self.assertEqual(Action.CREATE, Action.value_of("create"))
        self.assertEqual(Action.UPDATE, Action.value_of("update"))
        self.assertEqual(Action.DELETE, Action.value_of("delete"))

    def test_value_of_raises(self):
        with self.assertRaises(MinosModelException):
            Action.value_of("foo")

    def test_is_create(self):
        self.assertTrue(Action.CREATE.is_create)
        self.assertFalse(Action.UPDATE.is_create)
        self.assertFalse(Action.DELETE.is_create)

    def test_is_update(self):
        self.assertFalse(Action.CREATE.is_update)
        self.assertTrue(Action.UPDATE.is_update)
        self.assertFalse(Action.DELETE.is_update)

    def test_is_delete(self):
        self.assertFalse(Action.CREATE.is_delete)
        self.assertFalse(Action.UPDATE.is_delete)
        self.assertTrue(Action.DELETE.is_delete)


if __name__ == "__main__":
    unittest.main()
