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
from uuid import (
    uuid4,
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
        self.uuid = uuid4()

        self.container = containers.DynamicContainer()
        self.container.event_broker = providers.Singleton(FakeBroker)
        self.container.repository = providers.Singleton(FakeRepository)
        self.container.snapshot = providers.Singleton(FakeSnapshot)
        self.container.wire(modules=[sys.modules[__name__]])

    def tearDown(self) -> None:
        self.container.unwire()

    def test_constructor(self):
        entry = SnapshotEntry(self.uuid, "example.Car", 0, bytes("car", "utf-8"))
        self.assertEqual(self.uuid, entry.aggregate_uuid)
        self.assertEqual("example.Car", entry.aggregate_name)
        self.assertEqual(0, entry.version)
        self.assertEqual(bytes("car", "utf-8"), entry.data)
        self.assertEqual(None, entry.created_at)
        self.assertEqual(None, entry.updated_at)

    def test_constructor_extended(self):
        entry = SnapshotEntry(
            self.uuid,
            "example.Car",
            0,
            bytes("car", "utf-8"),
            datetime(2020, 1, 10, 4, 23),
            datetime(2020, 1, 10, 4, 25),
        )
        self.assertEqual(self.uuid, entry.aggregate_uuid)
        self.assertEqual("example.Car", entry.aggregate_name)
        self.assertEqual(0, entry.version)
        self.assertEqual(bytes("car", "utf-8"), entry.data)
        self.assertEqual(datetime(2020, 1, 10, 4, 23), entry.created_at)
        self.assertEqual(datetime(2020, 1, 10, 4, 25), entry.updated_at)

    def test_from_aggregate(self):
        car = Car(3, "blue", uuid=self.uuid, version=1)
        entry = SnapshotEntry.from_aggregate(car)
        self.assertEqual(car.uuid, entry.aggregate_uuid)
        self.assertEqual(car.classname, entry.aggregate_name)
        self.assertEqual(car.version, entry.version)
        self.assertIsInstance(entry.data, bytes)
        self.assertEqual(car.created_at, entry.created_at)
        self.assertEqual(car.updated_at, entry.updated_at)

    def test_equals(self):
        a = SnapshotEntry(self.uuid, "example.Car", 0, bytes("car", "utf-8"))
        b = SnapshotEntry(self.uuid, "example.Car", 0, bytes("car", "utf-8"))
        self.assertEqual(a, b)

    def test_hash(self):
        entry = SnapshotEntry(self.uuid, "example.Car", 0, bytes("car", "utf-8"))
        self.assertIsInstance(hash(entry), int)

    def test_aggregate_cls(self):
        car = Car(3, "blue", uuid=self.uuid, version=1)
        entry = SnapshotEntry.from_aggregate(car)
        self.assertEqual(Car, entry.aggregate_cls)

    def test_aggregate(self):
        car = Car(3, "blue", uuid=self.uuid, version=1)
        entry = SnapshotEntry.from_aggregate(car)
        self.assertEqual(car, entry.build_aggregate())

    def test_repr(self):
        entry = SnapshotEntry(
            self.uuid,
            "example.Car",
            0,
            bytes("car", "utf-8"),
            datetime(2020, 1, 10, 4, 23),
            datetime(2020, 1, 10, 4, 25),
        )
        expected = (
            f"SnapshotEntry(aggregate_uuid={self.uuid!r}, aggregate_name='example.Car', version=0, data=b'car', "
            "created_at=datetime.datetime(2020, 1, 10, 4, 23), updated_at=datetime.datetime(2020, 1, 10, 4, 25))"
        )
        self.assertEqual(expected, repr(entry))


if __name__ == "__main__":
    unittest.main()
