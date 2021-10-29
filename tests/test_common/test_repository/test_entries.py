import unittest
from datetime import (
    datetime,
)
from uuid import (
    uuid4,
)

from minos.common import (
    NULL_UUID,
    Action,
    AggregateDiff,
    EventRepositoryEntry,
    FieldDiff,
    FieldDiffContainer,
    Transaction,
    current_datetime,
)
from tests.aggregate_classes import (
    Car,
)


class TestRepositoryEntry(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.uuid = uuid4()
        self.transaction_uuid = uuid4()

    def test_constructor(self):
        entry = EventRepositoryEntry(self.uuid, "example.Car", 0, bytes("car", "utf-8"))
        self.assertEqual(self.uuid, entry.aggregate_uuid)
        self.assertEqual("example.Car", entry.aggregate_name)
        self.assertEqual(0, entry.version)
        self.assertEqual(bytes("car", "utf-8"), entry.data)
        self.assertEqual(None, entry.id)
        self.assertEqual(None, entry.action)
        self.assertEqual(None, entry.created_at)
        self.assertEqual(NULL_UUID, entry.transaction_uuid)

    def test_constructor_extended(self):
        entry = EventRepositoryEntry(
            aggregate_uuid=self.uuid,
            aggregate_name="example.Car",
            version=0,
            data=bytes("car", "utf-8"),
            id=5678,
            action=Action.CREATE,
            created_at=datetime(2020, 10, 13, 8, 45, 32),
            transaction_uuid=self.transaction_uuid,
        )
        self.assertEqual(self.uuid, entry.aggregate_uuid)
        self.assertEqual("example.Car", entry.aggregate_name)
        self.assertEqual(0, entry.version)
        self.assertEqual(bytes("car", "utf-8"), entry.data)
        self.assertEqual(5678, entry.id)
        self.assertEqual(Action.CREATE, entry.action)
        self.assertEqual(datetime(2020, 10, 13, 8, 45, 32), entry.created_at)
        self.assertEqual(self.transaction_uuid, entry.transaction_uuid)

    async def test_from_aggregate_diff(self):
        fields_diff = FieldDiffContainer([FieldDiff("doors", int, 3), FieldDiff("color", str, "blue")])
        created_at = current_datetime()
        aggregate_diff = AggregateDiff(self.uuid, Car.classname, 1, Action.CREATE, created_at, fields_diff)

        entry = EventRepositoryEntry.from_aggregate_diff(aggregate_diff)
        self.assertEqual(self.uuid, entry.aggregate_uuid)
        self.assertEqual("tests.aggregate_classes.Car", entry.aggregate_name)
        self.assertEqual(None, entry.version)
        self.assertEqual(fields_diff, FieldDiffContainer.from_avro_bytes(entry.data))
        self.assertEqual(None, entry.id)
        self.assertEqual(Action.CREATE, entry.action)
        self.assertEqual(None, entry.created_at)
        self.assertEqual(NULL_UUID, entry.transaction_uuid)

    async def test_from_aggregate_diff_with_transaction(self):
        transaction = Transaction(self.transaction_uuid)
        fields_diff = FieldDiffContainer([FieldDiff("doors", int, 3), FieldDiff("color", str, "blue")])
        created_at = current_datetime()
        aggregate_diff = AggregateDiff(self.uuid, Car.classname, 1, Action.CREATE, created_at, fields_diff)

        entry = EventRepositoryEntry.from_aggregate_diff(aggregate_diff, transaction=transaction)
        self.assertEqual(self.uuid, entry.aggregate_uuid)
        self.assertEqual("tests.aggregate_classes.Car", entry.aggregate_name)
        self.assertEqual(None, entry.version)
        self.assertEqual(fields_diff, FieldDiffContainer.from_avro_bytes(entry.data))
        self.assertEqual(None, entry.id)
        self.assertEqual(Action.CREATE, entry.action)
        self.assertEqual(None, entry.created_at)
        self.assertEqual(self.transaction_uuid, entry.transaction_uuid)

    async def test_from_another(self):
        created_at = datetime(2020, 10, 13, 8, 45, 32)
        another = EventRepositoryEntry(
            aggregate_uuid=self.uuid,
            aggregate_name="example.Car",
            version=0,
            data=bytes("car", "utf-8"),
            id=5678,
            action=Action.CREATE,
            created_at=created_at,
            transaction_uuid=self.transaction_uuid,
        )
        transaction_uuid = uuid4()
        entry = EventRepositoryEntry.from_another(another, transaction_uuid=transaction_uuid)

        self.assertEqual(self.uuid, entry.aggregate_uuid)
        self.assertEqual("example.Car", entry.aggregate_name)
        self.assertEqual(0, entry.version)
        self.assertEqual(bytes("car", "utf-8"), entry.data)
        self.assertEqual(None, entry.id)
        self.assertEqual(Action.CREATE, entry.action)
        self.assertEqual(created_at, entry.created_at)
        self.assertEqual(transaction_uuid, entry.transaction_uuid)

    def test_aggregate_diff(self):
        field_diff_container = FieldDiffContainer([FieldDiff("doors", int, 3), FieldDiff("color", str, "blue")])
        version = 1
        now = current_datetime()

        aggregate_diff = AggregateDiff(self.uuid, Car.classname, version, Action.CREATE, now, field_diff_container)

        entry = EventRepositoryEntry.from_aggregate_diff(aggregate_diff)

        entry.version = version
        entry.created_at = now

        self.assertEqual(aggregate_diff, entry.aggregate_diff)

    def test_field_diff_container(self):
        field_diff_container = FieldDiffContainer([FieldDiff("doors", int, 3), FieldDiff("color", str, "blue")])
        entry = EventRepositoryEntry(self.uuid, "example.Car", 0, field_diff_container.avro_bytes)

        self.assertEqual(field_diff_container, entry.field_diff_container)

    def test_field_diff_container_empty(self):
        entry = EventRepositoryEntry(self.uuid, "example.Car", 0, bytes())

        self.assertEqual(FieldDiffContainer.empty(), entry.field_diff_container)

    def test_id_setup(self):
        entry = EventRepositoryEntry(self.uuid, "example.Car", 0, bytes("car", "utf-8"))
        self.assertEqual(None, entry.id)
        entry.id = 5678
        self.assertEqual(5678, entry.id)

    def test_action_setup(self):
        entry = EventRepositoryEntry(self.uuid, "example.Car", 0, bytes("car", "utf-8"))
        self.assertEqual(None, entry.action)
        entry.action = Action.CREATE
        self.assertEqual(Action.CREATE, entry.action)

    def test_equals(self):
        a = EventRepositoryEntry(self.uuid, "example.Car", 0, bytes("car", "utf-8"))
        b = EventRepositoryEntry(self.uuid, "example.Car", 0, bytes("car", "utf-8"))
        self.assertEqual(a, b)

    def test_hash(self):
        entry = EventRepositoryEntry(self.uuid, "example.Car", 0, bytes("car", "utf-8"))
        self.assertIsInstance(hash(entry), int)

    def test_repr(self):
        id_ = 5678
        version = 0
        aggregate_name = "example.Car"
        data = bytes("car", "utf-8")
        action = Action.CREATE
        created_at = datetime(2020, 10, 13, 8, 45, 32)
        transaction_uuid = uuid4()
        entry = EventRepositoryEntry(
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
            f"EventRepositoryEntry(aggregate_uuid={self.uuid!r}, aggregate_name={aggregate_name!r}, "
            f"version={version!r}, len(data)={len(data)!r}, id={id_!r}, action={action!r}, created_at={created_at!r}, "
            f"transaction_uuid={transaction_uuid!r})"
        )
        self.assertEqual(expected, repr(entry))

    def test_as_raw(self):
        id_ = 5678
        version = 0
        aggregate_name = "example.Car"
        data = bytes("car", "utf-8")
        action = Action.CREATE
        created_at = datetime(2020, 10, 13, 8, 45, 32)
        transaction_uuid = uuid4()
        entry = EventRepositoryEntry(
            aggregate_uuid=self.uuid,
            aggregate_name=aggregate_name,
            version=version,
            data=data,
            id=id_,
            action=action,
            created_at=created_at,
            transaction_uuid=transaction_uuid,
        )
        expected = {
            "aggregate_uuid": self.uuid,
            "aggregate_name": aggregate_name,
            "version": version,
            "data": data,
            "id": id_,
            "action": action,
            "created_at": created_at,
            "transaction_uuid": transaction_uuid,
        }

        self.assertEqual(expected, entry.as_raw())


if __name__ == "__main__":
    unittest.main()
