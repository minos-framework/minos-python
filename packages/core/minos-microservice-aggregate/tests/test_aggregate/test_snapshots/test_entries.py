import json
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

from minos.aggregate import (
    AlreadyDeletedException,
    Delta,
    DeltaEntry,
    SnapshotEntry,
)
from minos.common import (
    AvroSchemaEncoder,
    MinosJsonBinaryProtocol,
    ModelType,
    classname,
)
from tests.utils import (
    AggregateTestCase,
    Car,
)


class TestSnapshotEntry(AggregateTestCase):
    def setUp(self) -> None:
        super().setUp()

        self.uuid = uuid4()

        self.schema = {
            "type": "record",
            "name": "example.Car",
            "fields": [{"name": "color", "type": "string"}],
        }
        self.data = {"color": "blue"}

    def test_constructor(self):
        entry = SnapshotEntry(self.uuid, Car, 0, self.schema, self.data)
        self.assertEqual(self.uuid, entry.uuid)
        self.assertEqual(Car, entry.type_)
        self.assertEqual(0, entry.version)
        self.assertEqual(self.schema, entry.schema)
        self.assertEqual(self.data, entry.data)
        self.assertEqual(None, entry.created_at)
        self.assertEqual(None, entry.updated_at)

    def test_constructor_with_kwargs(self):
        entry = SnapshotEntry(self.uuid, Car, 0, self.schema, foo="bar", foobar="foobar")
        self.assertEqual({"foo": "bar", "foobar": "foobar"}, entry.data)

    def test_constructor_with_data_and_kwargs(self):
        entry = SnapshotEntry(self.uuid, Car, 0, self.schema, {"foo": "bar"}, foobar="foobar")
        self.assertEqual({"foo": "bar", "foobar": "foobar"}, entry.data)

    def test_constructor_with_data_str(self):
        entry = SnapshotEntry(self.uuid, Car, 0, self.schema, json.dumps({"foo": "bar", "foobar": "foobar"}))
        self.assertEqual({"foo": "bar", "foobar": "foobar"}, entry.data)

    def test_constructor_with_bytes_schema(self):
        raw = MinosJsonBinaryProtocol.encode(self.schema)
        entry = SnapshotEntry(self.uuid, Car, 0, raw, self.data)
        self.assertEqual(self.schema, entry.schema)

    # noinspection SpellCheckingInspection
    def test_constructor_with_memoryview_schema(self):
        raw = memoryview(MinosJsonBinaryProtocol.encode(self.schema))
        entry = SnapshotEntry(self.uuid, Car, 0, raw, self.data)
        self.assertEqual(self.schema, entry.schema)

    def test_constructor_extended(self):
        entry = SnapshotEntry(
            self.uuid,
            Car,
            0,
            self.schema,
            self.data,
            datetime(2020, 1, 10, 4, 23),
            datetime(2020, 1, 10, 4, 25),
        )
        self.assertEqual(self.uuid, entry.uuid)
        self.assertEqual(Car, entry.type_)
        self.assertEqual(0, entry.version)
        self.assertEqual(self.schema, entry.schema)
        self.assertEqual(self.data, entry.data)
        self.assertEqual(datetime(2020, 1, 10, 4, 23), entry.created_at)
        self.assertEqual(datetime(2020, 1, 10, 4, 25), entry.updated_at)

    def test_from_entity(self):
        car = Car(3, "blue", uuid=self.uuid, version=1)
        with patch.object(AvroSchemaEncoder, "generate_random_str", return_value="hello"):
            entry = SnapshotEntry.from_entity(car)
            self.assertEqual(car.uuid, entry.uuid)
            self.assertEqual(Car, entry.type_)
            self.assertEqual(car.version, entry.version)
            self.assertEqual(car.avro_schema, entry.schema)
            self.assertEqual({"color": "blue", "doors": 3, "owner": None}, entry.data)
            self.assertEqual(car.created_at, entry.created_at)
            self.assertEqual(car.updated_at, entry.updated_at)

    def test_from_delta_entry(self):
        car = Car(3, "blue", uuid=self.uuid, version=1)
        delta_entry = DeltaEntry.from_delta(Delta.from_entity(car), version=1)
        with patch.object(AvroSchemaEncoder, "generate_random_str", return_value="hello"):
            snapshot_entry = SnapshotEntry.from_delta_entry(delta_entry)
            self.assertEqual(delta_entry.uuid, snapshot_entry.uuid)
            self.assertEqual(delta_entry.type_, snapshot_entry.type_)
            self.assertEqual(delta_entry.version, snapshot_entry.version)
            self.assertEqual(delta_entry.created_at, snapshot_entry.created_at)
            self.assertEqual(delta_entry.created_at, snapshot_entry.updated_at)
            self.assertEqual(delta_entry.transaction_uuid, snapshot_entry.transaction_uuid)

    def test_equals(self):
        a = SnapshotEntry(self.uuid, Car, 0, self.schema, self.data)
        b = SnapshotEntry(self.uuid, Car, 0, self.schema, self.data)
        self.assertEqual(a, b)

    def test_type_(self):
        car = Car(3, "blue", uuid=self.uuid, version=1)
        entry = SnapshotEntry.from_entity(car)
        self.assertEqual(Car, entry.type_)

    def test_build(self):
        car = Car(3, "blue", uuid=self.uuid, version=1)
        entry = SnapshotEntry.from_entity(car)
        self.assertEqual(car, entry.build())

    def test_build_raises(self):
        entry = SnapshotEntry(uuid=self.uuid, type_=Car, version=0, schema=self.schema)

        with self.assertRaises(AlreadyDeletedException):
            entry.build()

    def test_build_type_from_schema(self):
        entry = SnapshotEntry(self.uuid, Car, 0, Car.avro_schema)
        self.assertEqual(ModelType.from_model(Car), entry.build_type())

    def test_build_type_from_classname(self):
        # noinspection PyTypeChecker
        entry = SnapshotEntry(self.uuid, Car.classname, 0)
        self.assertEqual(ModelType.from_model(Car), entry.build_type())

    def test_repr(self):
        type_ = Car
        version = 0
        created_at = datetime(2020, 1, 10, 4, 23)
        updated_at = datetime(2020, 1, 10, 4, 25)
        transaction_uuid = uuid4()

        entry = SnapshotEntry(
            uuid=self.uuid,
            type_=type_,
            version=version,
            schema=self.schema,
            data=self.data,
            created_at=created_at,
            updated_at=updated_at,
            transaction_uuid=transaction_uuid,
        )

        expected = (
            f"SnapshotEntry(uuid={self.uuid!r}, type_={type_!r}, version={version!r}, "
            f"schema={self.schema!r}, data={self.data!r}, created_at={created_at!r}, updated_at={updated_at!r}, "
            f"transaction_uuid={transaction_uuid!r})"
        )

        self.assertEqual(expected, repr(entry))

    def test_as_raw(self):
        version = 0
        created_at = datetime(2020, 1, 10, 4, 23)
        updated_at = datetime(2020, 1, 10, 4, 25)
        transaction_uuid = uuid4()

        entry = SnapshotEntry(
            uuid=self.uuid,
            type_=Car,
            version=version,
            schema=self.schema,
            data=self.data,
            created_at=created_at,
            updated_at=updated_at,
            transaction_uuid=transaction_uuid,
        )

        expected = {
            "created_at": created_at,
            "data": self.data,
            "type_": classname(Car),
            "schema": self.schema,
            "transaction_uuid": transaction_uuid,
            "updated_at": updated_at,
            "uuid": self.uuid,
            "version": version,
        }

        self.assertEqual(expected, entry.as_raw())

    def test_encoded_schema(self):
        entry = SnapshotEntry(uuid=self.uuid, type_=Car, version=0, schema=self.schema)
        expected = MinosJsonBinaryProtocol.encode(self.schema)
        self.assertEqual(expected, entry.encoded_schema)

    def test_encoded_schema_none(self):
        entry = SnapshotEntry(uuid=self.uuid, type_=Car, version=0)
        self.assertEqual(None, entry.encoded_schema)

    def test_encoded_data(self):
        entry = SnapshotEntry(uuid=self.uuid, type_=Car, version=0, schema=self.schema, data=self.data)
        expected = json.dumps(self.data)
        self.assertEqual(expected, entry.encoded_data)

    def test_encoded_none(self):
        entry = SnapshotEntry(uuid=self.uuid, type_=Car, version=0, schema=self.schema)
        self.assertEqual(None, entry.encoded_data)


if __name__ == "__main__":
    unittest.main()
