"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""

import unittest
from datetime import (
    date,
    datetime,
    time,
)
from typing import (
    Union,
)
from uuid import (
    UUID,
)

from minos.common import (
    AvroSchemaDecoder,
    MinosMalformedAttributeException,
    ModelType,
)


class TestAvroSchemaDecoder(unittest.TestCase):
    def test_model_type(self):
        expected = ModelType.build("User", {"username": str}, "path.to")
        field_schema = {
            "fields": [{"name": "username", "type": "string"}],
            "name": "User",
            "namespace": "path.to.class",
            "type": "record",
        }

        observed = AvroSchemaDecoder(field_schema).build()
        self.assertEqual(expected, observed)

    def test_model_type_single_module(self):
        expected = ModelType.build("User", {"username": str}, "example")
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

    def test_uuid(self):
        expected = UUID
        observed = AvroSchemaDecoder({"name": "id", "type": "string", "logicalType": "uuid"}).build()
        self.assertEqual(expected, observed)

    def test_plain_array(self):
        expected = list[str]
        observed = AvroSchemaDecoder({"name": "example", "type": "array", "items": "string"}).build()
        self.assertEqual(expected, observed)

    def test_plain_map(self):
        expected = dict[str, int]
        observed = AvroSchemaDecoder({"name": "example", "type": "map", "values": "int"}).build()
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
        with self.assertRaises(MinosMalformedAttributeException):
            AvroSchemaDecoder({"name": "id", "type": "string", "logicalType": "foo"}).build()


if __name__ == "__main__":
    unittest.main()
