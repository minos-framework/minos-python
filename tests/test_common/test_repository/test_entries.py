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
        fields_diff = FieldDiffContainer([FieldDiff("doors", int, 3), FieldDiff("color", str, "blue")])
        created_at = current_datetime()
        aggregate_diff = AggregateDiff(self.uuid, Car.classname, 1, Action.CREATE, created_at, fields_diff)

        entry = RepositoryEntry.from_aggregate_diff(aggregate_diff)
        self.assertEqual(self.uuid, entry.aggregate_uuid)
        self.assertEqual("tests.aggregate_classes.Car", entry.aggregate_name)
        self.assertEqual(1, entry.version)
        self.assertEqual(fields_diff, FieldDiffContainer.from_avro_bytes(entry.data))
        self.assertEqual(None, entry.id)
        self.assertEqual(Action.CREATE, entry.action)
        self.assertEqual(created_at, entry.created_at)

    def test_aggregate_diff(self):
        field_diff_container = FieldDiffContainer([FieldDiff("doors", int, 3), FieldDiff("color", str, "blue")])
        aggregate_diff = AggregateDiff(
            self.uuid, Car.classname, 1, Action.CREATE, current_datetime(), field_diff_container
        )
        entry = RepositoryEntry.from_aggregate_diff(aggregate_diff)

        self.assertEqual(aggregate_diff, entry.aggregate_diff)

    def test_field_diff_container(self):
        field_diff_container = FieldDiffContainer([FieldDiff("doors", int, 3), FieldDiff("color", str, "blue")])
        entry = RepositoryEntry(self.uuid, "example.Car", 0, field_diff_container.avro_bytes)

        self.assertEqual(field_diff_container, entry.field_diff_container)

    def test_field_diff_container_empty(self):
        entry = RepositoryEntry(self.uuid, "example.Car", 0, bytes())

        self.assertEqual(FieldDiffContainer.empty(), entry.field_diff_container)

    def test_id_setup(self):
        entry = RepositoryEntry(self.uuid, "example.Car", 0, bytes("car", "utf-8"))
        self.assertEqual(None, entry.id)
        entry.id = 5678
        self.assertEqual(5678, entry.id)

    def test_action_setup(self):
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
        id_ = 5678
        version = 0
        aggregate_name = "example.Car"
        data = bytes("car", "utf-8")
        action = Action.CREATE
        created_at = datetime(2020, 10, 13, 8, 45, 32)
        transaction_uuid = uuid4()
        entry = RepositoryEntry(
            aggregate_uuid=self.uuid,
            aggregate_name=aggregate_name,
            version=version,
            data=data,
            id=id_,
            action=action,
            created_at=created_at,
            transaction_uuid=transaction_uuid,
        )
        expected = (
            f"RepositoryEntry(aggregate_uuid={self.uuid!r}, aggregate_name={aggregate_name!r}, version={version!r}, "
            f"data={data!r}, id={id_!r}, action={action!r}, created_at={created_at!r}, "
            f"transaction_uuid={transaction_uuid!r})"
        )
        self.assertEqual(expected, repr(entry))


if __name__ == "__main__":
    unittest.main()
