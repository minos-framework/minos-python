import unittest
from datetime import (
    datetime,
)
from uuid import (
    uuid4,
)

from minos.aggregate import (
    Action,
    Delta,
    DeltaEntry,
    FieldDiff,
    FieldDiffContainer,
)
from minos.common import (
    NULL_UUID,
    classname,
    current_datetime,
)
from minos.transactions import (
    TransactionEntry,
)
from tests.utils import (
    Car,
)


class TestDeltaEntry(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.uuid = uuid4()
        self.transaction_uuid = uuid4()

    def test_constructor(self):
        entry = DeltaEntry(self.uuid, Car, 0, bytes("car", "utf-8"))
        self.assertEqual(self.uuid, entry.uuid)
        self.assertEqual(Car, entry.type_)
        self.assertEqual(0, entry.version)
        self.assertEqual(bytes("car", "utf-8"), entry.data)
        self.assertEqual(None, entry.id)
        self.assertEqual(None, entry.action)
        self.assertEqual(None, entry.created_at)
        self.assertEqual(NULL_UUID, entry.transaction_uuid)

    # noinspection SpellCheckingInspection
    def test_constructor_with_memoryview_data(self):
        entry = DeltaEntry(self.uuid, Car, 0, memoryview(bytes("car", "utf-8")))
        self.assertEqual(bytes("car", "utf-8"), entry.data)

    def test_constructor_extended(self):
        entry = DeltaEntry(
            uuid=self.uuid,
            type_=Car,
            version=0,
            data=bytes("car", "utf-8"),
            id=5678,
            action=Action.CREATE,
            created_at=datetime(2020, 10, 13, 8, 45, 32),
            transaction_uuid=self.transaction_uuid,
        )
        self.assertEqual(self.uuid, entry.uuid)
        self.assertEqual(Car, entry.type_)
        self.assertEqual(0, entry.version)
        self.assertEqual(bytes("car", "utf-8"), entry.data)
        self.assertEqual(5678, entry.id)
        self.assertEqual(Action.CREATE, entry.action)
        self.assertEqual(datetime(2020, 10, 13, 8, 45, 32), entry.created_at)
        self.assertEqual(self.transaction_uuid, entry.transaction_uuid)

    async def test_from_delta(self):
        fields_diff = FieldDiffContainer([FieldDiff("doors", int, 3), FieldDiff("color", str, "blue")])
        created_at = current_datetime()
        delta = Delta(self.uuid, Car.classname, 1, Action.CREATE, created_at, fields_diff)

        entry = DeltaEntry.from_delta(delta)
        self.assertEqual(self.uuid, entry.uuid)
        self.assertEqual(Car, entry.type_)
        self.assertEqual(None, entry.version)
        self.assertEqual(fields_diff, FieldDiffContainer.from_avro_bytes(entry.data))
        self.assertEqual(None, entry.id)
        self.assertEqual(Action.CREATE, entry.action)
        self.assertEqual(None, entry.created_at)
        self.assertEqual(NULL_UUID, entry.transaction_uuid)

    async def test_from_delta_with_transaction(self):
        transaction = TransactionEntry(self.transaction_uuid)
        fields_diff = FieldDiffContainer([FieldDiff("doors", int, 3), FieldDiff("color", str, "blue")])
        created_at = current_datetime()
        delta = Delta(self.uuid, Car.classname, 1, Action.CREATE, created_at, fields_diff)

        entry = DeltaEntry.from_delta(delta, transaction=transaction)
        self.assertEqual(self.uuid, entry.uuid)
        self.assertEqual(Car, entry.type_)
        self.assertEqual(None, entry.version)
        self.assertEqual(fields_diff, FieldDiffContainer.from_avro_bytes(entry.data))
        self.assertEqual(None, entry.id)
        self.assertEqual(Action.CREATE, entry.action)
        self.assertEqual(None, entry.created_at)
        self.assertEqual(self.transaction_uuid, entry.transaction_uuid)

    async def test_from_another(self):
        created_at = datetime(2020, 10, 13, 8, 45, 32)
        another = DeltaEntry(
            uuid=self.uuid,
            type_=Car,
            version=0,
            data=bytes("car", "utf-8"),
            id=5678,
            action=Action.CREATE,
            created_at=created_at,
            transaction_uuid=self.transaction_uuid,
        )
        transaction_uuid = uuid4()
        entry = DeltaEntry.from_another(another, transaction_uuid=transaction_uuid)

        self.assertEqual(self.uuid, entry.uuid)
        self.assertEqual(Car, entry.type_)
        self.assertEqual(0, entry.version)
        self.assertEqual(bytes("car", "utf-8"), entry.data)
        self.assertEqual(None, entry.id)
        self.assertEqual(Action.CREATE, entry.action)
        self.assertEqual(created_at, entry.created_at)
        self.assertEqual(transaction_uuid, entry.transaction_uuid)

    def test_delta(self):
        field_diff_container = FieldDiffContainer([FieldDiff("doors", int, 3), FieldDiff("color", str, "blue")])
        version = 1
        now = current_datetime()

        delta = Delta(self.uuid, classname(Car), version, Action.CREATE, now, field_diff_container)

        entry = DeltaEntry.from_delta(delta)

        entry.version = version
        entry.created_at = now

        self.assertEqual(delta, entry.delta)

    def test_field_diff_container(self):
        field_diff_container = FieldDiffContainer([FieldDiff("doors", int, 3), FieldDiff("color", str, "blue")])
        entry = DeltaEntry(self.uuid, Car, 0, field_diff_container.avro_bytes)

        self.assertEqual(field_diff_container, entry.field_diff_container)

    def test_field_diff_container_empty(self):
        entry = DeltaEntry(self.uuid, Car, 0, bytes())

        self.assertEqual(FieldDiffContainer.empty(), entry.field_diff_container)

    def test_id_setup(self):
        entry = DeltaEntry(self.uuid, Car, 0, bytes("car", "utf-8"))
        self.assertEqual(None, entry.id)
        entry.id = 5678
        self.assertEqual(5678, entry.id)

    def test_action_setup(self):
        entry = DeltaEntry(self.uuid, Car, 0, bytes("car", "utf-8"))
        self.assertEqual(None, entry.action)
        entry.action = Action.CREATE
        self.assertEqual(Action.CREATE, entry.action)

    def test_equals(self):
        a = DeltaEntry(self.uuid, Car, 0, bytes("car", "utf-8"))
        b = DeltaEntry(self.uuid, Car, 0, bytes("car", "utf-8"))
        self.assertEqual(a, b)

    def test_hash(self):
        entry = DeltaEntry(self.uuid, Car, 0, bytes("car", "utf-8"))
        self.assertIsInstance(hash(entry), int)

    def test_repr(self):
        id_ = 5678
        version = 0
        name = Car
        data = bytes("car", "utf-8")
        action = Action.CREATE
        created_at = datetime(2020, 10, 13, 8, 45, 32)
        transaction_uuid = uuid4()
        entry = DeltaEntry(
            uuid=self.uuid,
            type_=name,
            version=version,
            data=data,
            id=id_,
            action=action,
            created_at=created_at,
            transaction_uuid=transaction_uuid,
        )
        expected = (
            f"DeltaEntry(uuid={self.uuid!r}, name={name!r}, "
            f"version={version!r}, len(data)={len(data)!r}, id={id_!r}, action={action!r}, created_at={created_at!r}, "
            f"transaction_uuid={transaction_uuid!r})"
        )
        self.assertEqual(expected, repr(entry))

    def test_as_raw(self):
        id_ = 5678
        version = 0
        type_ = Car
        data = bytes("car", "utf-8")
        action = Action.CREATE
        created_at = datetime(2020, 10, 13, 8, 45, 32)
        transaction_uuid = uuid4()
        entry = DeltaEntry(
            uuid=self.uuid,
            type_=type_,
            version=version,
            data=data,
            id=id_,
            action=action,
            created_at=created_at,
            transaction_uuid=transaction_uuid,
        )
        expected = {
            "uuid": self.uuid,
            "type_": classname(type_),
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
