import unittest
from datetime import (
    datetime,
)
from uuid import (
    uuid4,
)

from minos.aggregate import (
    Action,
    Event,
    EventEntry,
    FieldDiff,
    FieldDiffContainer,
    TransactionEntry,
)
from minos.common import (
    NULL_UUID,
    current_datetime,
)
from tests.utils import (
    Car,
)


class TestRepositoryEntry(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.uuid = uuid4()
        self.transaction_uuid = uuid4()

    def test_constructor(self):
        entry = EventEntry(self.uuid, "example.Car", 0, bytes("car", "utf-8"))
        self.assertEqual(self.uuid, entry.uuid)
        self.assertEqual("example.Car", entry.name)
        self.assertEqual(0, entry.version)
        self.assertEqual(bytes("car", "utf-8"), entry.data)
        self.assertEqual(None, entry.id)
        self.assertEqual(None, entry.action)
        self.assertEqual(None, entry.created_at)
        self.assertEqual(NULL_UUID, entry.transaction_uuid)

    def test_constructor_extended(self):
        entry = EventEntry(
            uuid=self.uuid,
            name="example.Car",
            version=0,
            data=bytes("car", "utf-8"),
            id=5678,
            action=Action.CREATE,
            created_at=datetime(2020, 10, 13, 8, 45, 32),
            transaction_uuid=self.transaction_uuid,
        )
        self.assertEqual(self.uuid, entry.uuid)
        self.assertEqual("example.Car", entry.name)
        self.assertEqual(0, entry.version)
        self.assertEqual(bytes("car", "utf-8"), entry.data)
        self.assertEqual(5678, entry.id)
        self.assertEqual(Action.CREATE, entry.action)
        self.assertEqual(datetime(2020, 10, 13, 8, 45, 32), entry.created_at)
        self.assertEqual(self.transaction_uuid, entry.transaction_uuid)

    async def test_from_event(self):
        fields_diff = FieldDiffContainer([FieldDiff("doors", int, 3), FieldDiff("color", str, "blue")])
        created_at = current_datetime()
        event = Event(self.uuid, Car.classname, 1, Action.CREATE, created_at, fields_diff)

        entry = EventEntry.from_event(event)
        self.assertEqual(self.uuid, entry.uuid)
        self.assertEqual("tests.utils.Car", entry.name)
        self.assertEqual(None, entry.version)
        self.assertEqual(fields_diff, FieldDiffContainer.from_avro_bytes(entry.data))
        self.assertEqual(None, entry.id)
        self.assertEqual(Action.CREATE, entry.action)
        self.assertEqual(None, entry.created_at)
        self.assertEqual(NULL_UUID, entry.transaction_uuid)

    async def test_from_event_with_transaction(self):
        transaction = TransactionEntry(self.transaction_uuid)
        fields_diff = FieldDiffContainer([FieldDiff("doors", int, 3), FieldDiff("color", str, "blue")])
        created_at = current_datetime()
        event = Event(self.uuid, Car.classname, 1, Action.CREATE, created_at, fields_diff)

        entry = EventEntry.from_event(event, transaction=transaction)
        self.assertEqual(self.uuid, entry.uuid)
        self.assertEqual("tests.utils.Car", entry.name)
        self.assertEqual(None, entry.version)
        self.assertEqual(fields_diff, FieldDiffContainer.from_avro_bytes(entry.data))
        self.assertEqual(None, entry.id)
        self.assertEqual(Action.CREATE, entry.action)
        self.assertEqual(None, entry.created_at)
        self.assertEqual(self.transaction_uuid, entry.transaction_uuid)

    async def test_from_another(self):
        created_at = datetime(2020, 10, 13, 8, 45, 32)
        another = EventEntry(
            uuid=self.uuid,
            name="example.Car",
            version=0,
            data=bytes("car", "utf-8"),
            id=5678,
            action=Action.CREATE,
            created_at=created_at,
            transaction_uuid=self.transaction_uuid,
        )
        transaction_uuid = uuid4()
        entry = EventEntry.from_another(another, transaction_uuid=transaction_uuid)

        self.assertEqual(self.uuid, entry.uuid)
        self.assertEqual("example.Car", entry.name)
        self.assertEqual(0, entry.version)
        self.assertEqual(bytes("car", "utf-8"), entry.data)
        self.assertEqual(None, entry.id)
        self.assertEqual(Action.CREATE, entry.action)
        self.assertEqual(created_at, entry.created_at)
        self.assertEqual(transaction_uuid, entry.transaction_uuid)

    def test_event(self):
        field_diff_container = FieldDiffContainer([FieldDiff("doors", int, 3), FieldDiff("color", str, "blue")])
        version = 1
        now = current_datetime()

        event = Event(self.uuid, Car.classname, version, Action.CREATE, now, field_diff_container)

        entry = EventEntry.from_event(event)

        entry.version = version
        entry.created_at = now

        self.assertEqual(event, entry.event)

    def test_field_diff_container(self):
        field_diff_container = FieldDiffContainer([FieldDiff("doors", int, 3), FieldDiff("color", str, "blue")])
        entry = EventEntry(self.uuid, "example.Car", 0, field_diff_container.avro_bytes)

        self.assertEqual(field_diff_container, entry.field_diff_container)

    def test_field_diff_container_empty(self):
        entry = EventEntry(self.uuid, "example.Car", 0, bytes())

        self.assertEqual(FieldDiffContainer.empty(), entry.field_diff_container)

    def test_id_setup(self):
        entry = EventEntry(self.uuid, "example.Car", 0, bytes("car", "utf-8"))
        self.assertEqual(None, entry.id)
        entry.id = 5678
        self.assertEqual(5678, entry.id)

    def test_action_setup(self):
        entry = EventEntry(self.uuid, "example.Car", 0, bytes("car", "utf-8"))
        self.assertEqual(None, entry.action)
        entry.action = Action.CREATE
        self.assertEqual(Action.CREATE, entry.action)

    def test_equals(self):
        a = EventEntry(self.uuid, "example.Car", 0, bytes("car", "utf-8"))
        b = EventEntry(self.uuid, "example.Car", 0, bytes("car", "utf-8"))
        self.assertEqual(a, b)

    def test_hash(self):
        entry = EventEntry(self.uuid, "example.Car", 0, bytes("car", "utf-8"))
        self.assertIsInstance(hash(entry), int)

    def test_repr(self):
        id_ = 5678
        version = 0
        name = "example.Car"
        data = bytes("car", "utf-8")
        action = Action.CREATE
        created_at = datetime(2020, 10, 13, 8, 45, 32)
        transaction_uuid = uuid4()
        entry = EventEntry(
            uuid=self.uuid,
            name=name,
            version=version,
            data=data,
            id=id_,
            action=action,
            created_at=created_at,
            transaction_uuid=transaction_uuid,
        )
        expected = (
            f"EventEntry(uuid={self.uuid!r}, name={name!r}, "
            f"version={version!r}, len(data)={len(data)!r}, id={id_!r}, action={action!r}, created_at={created_at!r}, "
            f"transaction_uuid={transaction_uuid!r})"
        )
        self.assertEqual(expected, repr(entry))

    def test_as_raw(self):
        id_ = 5678
        version = 0
        name = "example.Car"
        data = bytes("car", "utf-8")
        action = Action.CREATE
        created_at = datetime(2020, 10, 13, 8, 45, 32)
        transaction_uuid = uuid4()
        entry = EventEntry(
            uuid=self.uuid,
            name=name,
            version=version,
            data=data,
            id=id_,
            action=action,
            created_at=created_at,
            transaction_uuid=transaction_uuid,
        )
        expected = {
            "uuid": self.uuid,
            "name": name,
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
