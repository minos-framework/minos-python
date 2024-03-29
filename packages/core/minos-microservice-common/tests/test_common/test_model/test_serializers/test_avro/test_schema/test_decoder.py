import unittest
from datetime import (
    date,
    datetime,
    time,
    timedelta,
)
from typing import (
    Any,
    Union,
)
from unittest.mock import (
    patch,
)
from uuid import (
    UUID,
)

from minos.common import (
    AvroSchemaDecoder,
    MinosMalformedAttributeException,
    ModelType,
    classname,
)
from tests.model_classes import (
    ShoppingList,
    Status,
)


class TestAvroSchemaDecoder(unittest.TestCase):
    def test_model_type(self):
        expected = ModelType.build("User", {"username": str}, namespace_="path.to")
        field_schema = {
            "fields": [{"name": "username", "type": "string"}],
            "name": "User",
            "namespace": "path.to.class",
            "type": "record",
        }

        observed = AvroSchemaDecoder(field_schema).build()
        self.assertEqual(expected, observed)

    def test_model_type_single_module(self):
        expected = ModelType.build("User", {"username": str}, namespace_="example")
        field_schema = {
            "fields": [{"name": "username", "type": "string"}],
            "name": "User",
            "namespace": "example",
            "type": "record",
        }

        observed = AvroSchemaDecoder(field_schema).build()
        self.assertEqual(expected, observed)

    def test_int(self):
        expected = int
        observed = AvroSchemaDecoder({"name": "id", "type": "int"}).build()
        self.assertEqual(expected, observed)

    def test_bool(self):
        expected = bool
        observed = AvroSchemaDecoder({"name": "id", "type": "boolean"}).build()
        self.assertEqual(expected, observed)

    def test_float(self):
        expected = float
        observed = AvroSchemaDecoder({"name": "id", "type": "float"}).build()
        self.assertEqual(expected, observed)

    def test_double(self):
        expected = float
        observed = AvroSchemaDecoder({"name": "id", "type": "double"}).build()
        self.assertEqual(expected, observed)

    def test_bytes(self):
        expected = bytes
        observed = AvroSchemaDecoder({"name": "id", "type": "bytes"}).build()
        self.assertEqual(expected, observed)

    def test_date(self):
        expected = date
        observed = AvroSchemaDecoder({"name": "id", "type": "int", "logicalType": "date"}).build()
        self.assertEqual(expected, observed)

    def test_time(self):
        expected = time
        observed = AvroSchemaDecoder({"name": "id", "type": "int", "logicalType": "time-micros"}).build()
        self.assertEqual(expected, observed)

    def test_datetime(self):
        expected = datetime
        observed = AvroSchemaDecoder({"name": "id", "type": "long", "logicalType": "timestamp-micros"}).build()
        self.assertEqual(expected, observed)

    def test_timedelta(self):
        expected = timedelta
        observed = AvroSchemaDecoder({"name": "id", "type": "long", "logicalType": "timedelta-micros"}).build()
        self.assertEqual(expected, observed)

    def test_uuid(self):
        expected = UUID
        observed = AvroSchemaDecoder({"name": "id", "type": "string", "logicalType": "uuid"}).build()
        self.assertEqual(expected, observed)

    def test_list(self):
        expected = list[str]
        observed = AvroSchemaDecoder({"name": "example", "type": "array", "items": "string"}).build()
        self.assertEqual(expected, observed)

    def test_list_any(self):
        expected = list[Any]
        observed = AvroSchemaDecoder({"name": "example", "type": "array", "items": "null"}).build()
        self.assertEqual(expected, observed)

    def test_set(self):
        expected = set[str]
        schema = {"name": "example", "type": "array", "items": "string", "logicalType": "set"}
        observed = AvroSchemaDecoder(schema).build()
        self.assertEqual(expected, observed)

    def test_set_any(self):
        expected = set[Any]
        schema = {"name": "example", "type": "array", "items": "null", "logicalType": "set"}
        observed = AvroSchemaDecoder(schema).build()
        self.assertEqual(expected, observed)

    def test_dict(self):
        expected = dict[str, int]
        observed = AvroSchemaDecoder({"name": "example", "type": "map", "values": "int"}).build()
        self.assertEqual(expected, observed)

    def test_dict_any(self):
        expected = dict[str, Any]
        observed = AvroSchemaDecoder({"name": "example", "type": "map", "values": "null"}).build()
        self.assertEqual(expected, observed)

    def test_nested_arrays(self):
        expected = list[list[str]]
        observed = AvroSchemaDecoder(
            {"name": "example", "type": "array", "items": {"type": {"type": "array", "items": "string"}}},
        ).build()
        self.assertEqual(expected, observed)

    def test_none(self):
        expected = type(None)
        observed = AvroSchemaDecoder({"name": "example", "type": "null"}).build()
        self.assertEqual(expected, observed)

    def test_union(self):
        expected = list[Union[int, str]]
        observed = AvroSchemaDecoder({"name": "example", "type": "array", "items": ["int", "string"]}).build()
        self.assertEqual(expected, observed)

    def test_raises(self):
        with self.assertRaises(MinosMalformedAttributeException):
            AvroSchemaDecoder({"name": "id", "type": "foo"}).build()

    def test_logical_type(self):
        expected = Status
        observed = AvroSchemaDecoder({"type": "string", "logicalType": classname(Status)}).build()
        self.assertEqual(expected, observed)

    def test_logical_type_unknown(self):
        expected = str
        observed = AvroSchemaDecoder({"name": "id", "type": "string", "logicalType": "foo"}).build()
        self.assertEqual(expected, observed)

    def test_logical_type_model(self):
        with patch.object(ShoppingList, "decode_schema", return_value=ShoppingList):
            observed = AvroSchemaDecoder({"type": "string", "logicalType": classname(ShoppingList)}).build()
        self.assertEqual(ShoppingList, observed)


if __name__ == "__main__":
    unittest.main()
