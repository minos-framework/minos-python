"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import sys
import unittest
from datetime import (
    datetime,
)

from dependency_injector import (
    containers,
    providers,
)

from minos.common import (
    SnapshotEntry,
)
from tests.aggregate_classes import (
    Car,
)
from tests.utils import (
    FakeBroker,
    FakeRepository,
    FakeSnapshot,
)


class TestSnapshotEntry(unittest.TestCase):
    def setUp(self) -> None:
        self.container = containers.DynamicContainer()
        self.container.event_broker = providers.Singleton(FakeBroker)
        self.container.repository = providers.Singleton(FakeRepository)
        self.container.snapshot = providers.Singleton(FakeSnapshot)
        self.container.wire(modules=[sys.modules[__name__]])

    def tearDown(self) -> None:
        self.container.unwire()

    def test_constructor(self):
        entry = SnapshotEntry(1234, "example.Car", 0, bytes("car", "utf-8"))
        self.assertEqual(1234, entry.aggregate_id)
        self.assertEqual("example.Car", entry.aggregate_name)
        self.assertEqual(0, entry.version)
        self.assertEqual(bytes("car", "utf-8"), entry.data)
        self.assertEqual(None, entry.created_at)
        self.assertEqual(None, entry.updated_at)

    def test_constructor_extended(self):
        entry = SnapshotEntry(
            1234, "example.Car", 0, bytes("car", "utf-8"), datetime(2020, 1, 10, 4, 23), datetime(2020, 1, 10, 4, 25)
        )
        self.assertEqual(1234, entry.aggregate_id)
        self.assertEqual("example.Car", entry.aggregate_name)
        self.assertEqual(0, entry.version)
        self.assertEqual(bytes("car", "utf-8"), entry.data)
        self.assertEqual(datetime(2020, 1, 10, 4, 23), entry.created_at)
        self.assertEqual(datetime(2020, 1, 10, 4, 25), entry.updated_at)

    def test_from_aggregate(self):
        car = Car(1, 1, 3, "blue")
        entry = SnapshotEntry.from_aggregate(car)
        self.assertEqual(car.id, entry.aggregate_id)
        self.assertEqual(car.classname, entry.aggregate_name)
        self.assertEqual(car.version, entry.version)
        self.assertIsInstance(entry.data, bytes)
        self.assertEqual(None, entry.created_at)
        self.assertEqual(None, entry.updated_at)

    def test_equals(self):
        a = SnapshotEntry(1234, "example.Car", 0, bytes("car", "utf-8"))
        b = SnapshotEntry(1234, "example.Car", 0, bytes("car", "utf-8"))
        self.assertEqual(a, b)

    def test_hash(self):
        entry = SnapshotEntry(1234, "example.Car", 0, bytes("car", "utf-8"))
        self.assertIsInstance(hash(entry), int)

    def test_aggregate_cls(self):
        car = Car(1, 1, 3, "blue")
        entry = SnapshotEntry.from_aggregate(car)
        self.assertEqual(Car, entry.aggregate_cls)

    def test_aggregate(self):
        car = Car(1, 1, 3, "blue")
        entry = SnapshotEntry.from_aggregate(car)
        self.assertEqual(car, entry.aggregate)

    def test_repr(self):
        entry = SnapshotEntry(
            1234, "example.Car", 0, bytes("car", "utf-8"), datetime(2020, 1, 10, 4, 23), datetime(2020, 1, 10, 4, 25),
        )
        expected = (
            "SnapshotEntry(aggregate_id=1234, aggregate_name='example.Car', version=0, data=b'car', "
            "created_at=datetime.datetime(2020, 1, 10, 4, 23), updated_at=datetime.datetime(2020, 1, 10, 4, 25))"
        )
        self.assertEqual(expected, repr(entry))


if __name__ == "__main__":
    unittest.main()
