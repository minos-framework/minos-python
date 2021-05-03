"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import unittest
from datetime import (
    datetime,
)

from minos.common import (
    MinosRepositoryAction,
    MinosRepositoryEntry,
    MinosRepositoryUnknownActionException,
)
from tests.aggregate_classes import (
    Car,
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
        self.assertEqual(None, entry.created_at)

    def test_constructor_extended(self):
        entry = MinosRepositoryEntry(
            aggregate_id=1234,
            aggregate_name="example.Car",
            version=0,
            data=bytes("car", "utf-8"),
            id=5678,
            action=MinosRepositoryAction.INSERT,
            created_at=datetime(2020, 10, 13, 8, 45, 32),
        )
        self.assertEqual(1234, entry.aggregate_id)
        self.assertEqual("example.Car", entry.aggregate_name)
        self.assertEqual(0, entry.version)
        self.assertEqual(bytes("car", "utf-8"), entry.data)
        self.assertEqual(5678, entry.id)
        self.assertEqual(MinosRepositoryAction.INSERT, entry.action)
        self.assertEqual(datetime(2020, 10, 13, 8, 45, 32), entry.created_at)

    def test_from_aggregate(self):
        car = Car(1, 1, 3, "blue")
        entry = MinosRepositoryEntry.from_aggregate(car)
        self.assertEqual(car.id, entry.aggregate_id)
        self.assertEqual(car.classname, entry.aggregate_name)
        self.assertEqual(car.version, entry.version)
        self.assertIsInstance(entry.data, bytes)
        self.assertEqual(None, entry.id)
        self.assertEqual(None, entry.action)
        self.assertEqual(None, entry.created_at)

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

    def test_equals(self):
        a = MinosRepositoryEntry(1234, "example.Car", 0, bytes("car", "utf-8"))
        b = MinosRepositoryEntry(1234, "example.Car", 0, bytes("car", "utf-8"))
        self.assertEqual(a, b)

    def test_hash(self):
        entry = MinosRepositoryEntry(1234, "example.Car", 0, bytes("car", "utf-8"))
        self.assertIsInstance(hash(entry), int)

    def test_repr(self):
        entry = MinosRepositoryEntry(
            aggregate_id=1234,
            aggregate_name="example.Car",
            version=0,
            data=bytes("car", "utf-8"),
            id=5678,
            action=MinosRepositoryAction.INSERT,
            created_at=datetime(2020, 10, 13, 8, 45, 32),
        )
        expected = (
            "MinosRepositoryEntry(aggregate_id=1234, aggregate_name='example.Car', version=0, data=b'car', id=5678, "
            "action=<MinosRepositoryAction.INSERT: 'insert'>, created_at=datetime.datetime(2020, 10, 13, 8, 45, 32))"
        )
        self.assertEqual(expected, repr(entry))


if __name__ == "__main__":
    unittest.main()
