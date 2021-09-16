import sys
import unittest
from datetime import (
    datetime,
)
from unittest.mock import (
    patch,
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

        self.schema = {
            "type": "record",
            "name": "example.Car",
            "fields": [{"name": "color", "type": "string"}],
        }
        self.data = {"color": "blue"}

    def tearDown(self) -> None:
        self.container.unwire()

    def test_constructor(self):
        entry = SnapshotEntry(self.uuid, "example.Car", 0, self.schema, self.data)
        self.assertEqual(self.uuid, entry.aggregate_uuid)
        self.assertEqual("example.Car", entry.aggregate_name)
        self.assertEqual(0, entry.version)
        self.assertEqual(self.schema, entry.schema)
        self.assertEqual(self.data, entry.data)
        self.assertEqual(None, entry.created_at)
        self.assertEqual(None, entry.updated_at)

    def test_constructor_extended(self):
        entry = SnapshotEntry(
            self.uuid,
            "example.Car",
            0,
            self.schema,
            self.data,
            datetime(2020, 1, 10, 4, 23),
            datetime(2020, 1, 10, 4, 25),
        )
        self.assertEqual(self.uuid, entry.aggregate_uuid)
        self.assertEqual("example.Car", entry.aggregate_name)
        self.assertEqual(0, entry.version)
        self.assertEqual(self.schema, entry.schema)
        self.assertEqual(self.data, entry.data)
        self.assertEqual(datetime(2020, 1, 10, 4, 23), entry.created_at)
        self.assertEqual(datetime(2020, 1, 10, 4, 25), entry.updated_at)

    def test_from_aggregate(self):
        car = Car(3, "blue", uuid=self.uuid, version=1)
        with patch("minos.common.AvroSchemaEncoder.generate_random_str", return_value="hello"):
            entry = SnapshotEntry.from_aggregate(car)
            self.assertEqual(car.uuid, entry.aggregate_uuid)
            self.assertEqual(car.classname, entry.aggregate_name)
            self.assertEqual(car.version, entry.version)
            self.assertEqual(car.avro_schema, entry.schema)
            self.assertEqual({"color": "blue", "doors": 3, "owner": None}, entry.data)
            self.assertEqual(car.created_at, entry.created_at)
            self.assertEqual(car.updated_at, entry.updated_at)

    def test_equals(self):
        a = SnapshotEntry(self.uuid, "example.Car", 0, self.schema, self.data)
        b = SnapshotEntry(self.uuid, "example.Car", 0, self.schema, self.data)
        self.assertEqual(a, b)

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
            self.schema,
            self.data,
            datetime(2020, 1, 10, 4, 23),
            datetime(2020, 1, 10, 4, 25),
        )
        expected = (
            f"SnapshotEntry(aggregate_uuid={self.uuid!r}, aggregate_name='example.Car', version=0, "
            f"schema={self.schema!r}, data={self.data!r}, "
            "created_at=datetime.datetime(2020, 1, 10, 4, 23), updated_at=datetime.datetime(2020, 1, 10, 4, 25))"
        )
        self.assertEqual(expected, repr(entry))


if __name__ == "__main__":
    unittest.main()
