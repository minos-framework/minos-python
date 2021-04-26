"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import unittest

from minos.common import (
    MinosRepositoryAction,
    MinosRepositoryEntry,
    MinosRepositoryUnknownActionException,
)


class TestMinosRepositoryAction(unittest.TestCase):
    def test_value_of(self):
        self.assertEqual(MinosRepositoryAction.INSERT, MinosRepositoryAction.value_of("insert"))
        self.assertEqual(MinosRepositoryAction.UPDATE, MinosRepositoryAction.value_of("update"))
        self.assertEqual(MinosRepositoryAction.DELETE, MinosRepositoryAction.value_of("delete"))

    def test_value_of_raises(self):
        with self.assertRaises(MinosRepositoryUnknownActionException):
            MinosRepositoryAction.value_of("foo")


class TestMinosRepositoryEntry(unittest.TestCase):
    def test_constructor(self):
        entry = MinosRepositoryEntry(1234, "example.Car", 0, bytes("car", "utf-8"))
        self.assertEqual(1234, entry.aggregate_id)
        self.assertEqual("example.Car", entry.aggregate_name)
        self.assertEqual(0, entry.version)
        self.assertEqual(bytes("car", "utf-8"), entry.data)
        self.assertEqual(None, entry.id)
        self.assertEqual(None, entry.action)

    def test_constructor_extended(self):
        entry = MinosRepositoryEntry(1234, "example.Car", 0, bytes("car", "utf-8"), 5678, MinosRepositoryAction.INSERT)
        self.assertEqual(1234, entry.aggregate_id)
        self.assertEqual("example.Car", entry.aggregate_name)
        self.assertEqual(0, entry.version)
        self.assertEqual(bytes("car", "utf-8"), entry.data)
        self.assertEqual(5678, entry.id)
        self.assertEqual(MinosRepositoryAction.INSERT, entry.action)

    def test_id_set(self):
        entry = MinosRepositoryEntry(1234, "example.Car", 0, bytes("car", "utf-8"))
        self.assertEqual(None, entry.id)
        entry.id = 5678
        self.assertEqual(5678, entry.id)

    def test_id_action(self):
        entry = MinosRepositoryEntry(1234, "example.Car", 0, bytes("car", "utf-8"))
        self.assertEqual(None, entry.action)
        entry.action = MinosRepositoryAction.INSERT
        self.assertEqual(MinosRepositoryAction.INSERT, entry.action)


if __name__ == "__main__":
    unittest.main()
