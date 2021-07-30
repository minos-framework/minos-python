"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import unittest

from minos.common import (
    AggregateAction,
    MinosModelException,
)


class TestAggregateAction(unittest.TestCase):
    def test_value_of(self):
        self.assertEqual(AggregateAction.CREATE, AggregateAction.value_of("create"))
        self.assertEqual(AggregateAction.UPDATE, AggregateAction.value_of("update"))
        self.assertEqual(AggregateAction.DELETE, AggregateAction.value_of("delete"))

    def test_value_of_raises(self):
        with self.assertRaises(MinosModelException):
            AggregateAction.value_of("foo")

    def test_is_create(self):
        self.assertTrue(AggregateAction.CREATE.is_create)
        self.assertFalse(AggregateAction.UPDATE.is_create)
        self.assertFalse(AggregateAction.DELETE.is_create)

    def test_is_update(self):
        self.assertFalse(AggregateAction.CREATE.is_update)
        self.assertTrue(AggregateAction.UPDATE.is_update)
        self.assertFalse(AggregateAction.DELETE.is_update)

    def test_is_delete(self):
        self.assertFalse(AggregateAction.CREATE.is_delete)
        self.assertFalse(AggregateAction.UPDATE.is_delete)
        self.assertTrue(AggregateAction.DELETE.is_delete)


if __name__ == "__main__":
    unittest.main()
