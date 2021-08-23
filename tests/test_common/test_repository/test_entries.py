"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import unittest
from datetime import (
    datetime,
)
from uuid import (
    uuid4,
)

from minos.common import (
    Action,
    AggregateDiff,
    FieldDiff,
    FieldDiffContainer,
    RepositoryEntry,
    current_datetime,
)
from tests.aggregate_classes import (
    Car,
)


class TestRepositoryEntry(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.uuid = uuid4()

    def test_constructor(self):
        entry = RepositoryEntry(self.uuid, "example.Car", 0, bytes("car", "utf-8"))
        self.assertEqual(self.uuid, entry.aggregate_uuid)
        self.assertEqual("example.Car", entry.aggregate_name)
        self.assertEqual(0, entry.version)
        self.assertEqual(bytes("car", "utf-8"), entry.data)
        self.assertEqual(None, entry.id)
        self.assertEqual(None, entry.action)
        self.assertEqual(None, entry.created_at)

    def test_constructor_extended(self):
        entry = RepositoryEntry(
            aggregate_uuid=self.uuid,
            aggregate_name="example.Car",
            version=0,
            data=bytes("car", "utf-8"),
            id=5678,
            action=Action.CREATE,
            created_at=datetime(2020, 10, 13, 8, 45, 32),
        )
        self.assertEqual(self.uuid, entry.aggregate_uuid)
        self.assertEqual("example.Car", entry.aggregate_name)
        self.assertEqual(0, entry.version)
        self.assertEqual(bytes("car", "utf-8"), entry.data)
        self.assertEqual(5678, entry.id)
        self.assertEqual(Action.CREATE, entry.action)
        self.assertEqual(datetime(2020, 10, 13, 8, 45, 32), entry.created_at)

    async def test_from_aggregate_diff(self):
        differences = FieldDiffContainer([FieldDiff("doors", int, 3), FieldDiff("color", str, "blue")])
        aggregate_diff = AggregateDiff(
            uuid=self.uuid,
            name=Car.classname,
            version=1,
            action=Action.CREATE,
            created_at=current_datetime(),
            fields_diff=differences,
        )

        entry = RepositoryEntry.from_aggregate_diff(aggregate_diff)
        self.assertEqual(self.uuid, entry.aggregate_uuid)
        self.assertEqual("tests.aggregate_classes.Car", entry.aggregate_name)
        self.assertEqual(1, entry.version)
        self.assertEqual(differences, FieldDiffContainer.from_avro_bytes(entry.data))
        self.assertEqual(None, entry.id)
        self.assertEqual(None, entry.action)
        self.assertEqual(None, entry.created_at)

    def test_id_set(self):
        entry = RepositoryEntry(self.uuid, "example.Car", 0, bytes("car", "utf-8"))
        self.assertEqual(None, entry.id)
        entry.id = 5678
        self.assertEqual(5678, entry.id)

    def test_id_action(self):
        entry = RepositoryEntry(self.uuid, "example.Car", 0, bytes("car", "utf-8"))
        self.assertEqual(None, entry.action)
        entry.action = Action.CREATE
        self.assertEqual(Action.CREATE, entry.action)

    def test_equals(self):
        a = RepositoryEntry(self.uuid, "example.Car", 0, bytes("car", "utf-8"))
        b = RepositoryEntry(self.uuid, "example.Car", 0, bytes("car", "utf-8"))
        self.assertEqual(a, b)

    def test_hash(self):
        entry = RepositoryEntry(self.uuid, "example.Car", 0, bytes("car", "utf-8"))
        self.assertIsInstance(hash(entry), int)

    def test_repr(self):
        entry = RepositoryEntry(
            aggregate_uuid=self.uuid,
            aggregate_name="example.Car",
            version=0,
            data=bytes("car", "utf-8"),
            id=5678,
            action=Action.CREATE,
            created_at=datetime(2020, 10, 13, 8, 45, 32),
        )
        expected = (
            f"RepositoryEntry(aggregate_uuid={self.uuid!r}, aggregate_name='example.Car', version=0, data=b'car', "
            "id=5678, action=<Action.CREATE: 'create'>, created_at=datetime.datetime(2020, 10, 13, 8, 45, 32))"
        )
        self.assertEqual(expected, repr(entry))


if __name__ == "__main__":
    unittest.main()
