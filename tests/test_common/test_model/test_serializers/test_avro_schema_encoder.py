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
    Any,
    Optional,
)
from unittest.mock import (
    MagicMock,
)
from uuid import (
    UUID,
)

from minos.common import (
    AvroSchemaEncoder,
    ModelRef,
    ModelType,
)
from tests.aggregate_classes import (
    Owner,
)
from tests.model_classes import (
    User,
)


class TestAvroSchemaEncoder(unittest.TestCase):
    def test_model_type(self):
        expected = {
            "name": "class",
            "type": {
                "fields": [{"name": "username", "type": "string"}],
                "name": "User",
                "namespace": "path.to.hello",
                "type": "record",
            },
        }
        encoder = AvroSchemaEncoder("class", ModelType.build("User", {"username": str}, namespace_="path.to"))
        encoder.generate_random_str = MagicMock(return_value="hello")
        observed = encoder.build()
        self.assertEqual(expected, observed)

    def test_int(self):
        observed = AvroSchemaEncoder("test", int).build()
        expected = {"name": "test", "type": "int"}
        self.assertEqual(expected, observed)

    def test_bool(self):
        expected = {"name": "test", "type": "boolean"}
        observed = AvroSchemaEncoder("test", bool).build()
        self.assertEqual(expected, observed)

    def test_float(self):
        expected = {"name": "test", "type": "double"}
        observed = AvroSchemaEncoder("test", float).build()
        self.assertEqual(expected, observed)

    def test_string(self):
        expected = {"name": "test", "type": "string"}
        observed = AvroSchemaEncoder("test", str).build()
        self.assertEqual(expected, observed)

    def test_bytes(self):
        expected = {"name": "test", "type": "bytes"}
        observed = AvroSchemaEncoder("test", bytes).build()
        self.assertEqual(expected, observed)

    def test_date(self):
        expected = {"name": "test", "type": {"type": "int", "logicalType": "date"}}
        observed = AvroSchemaEncoder("test", date).build()
        self.assertEqual(expected, observed)

    def test_time(self):
        expected = {"name": "test", "type": {"type": "int", "logicalType": "time-micros"}}
        observed = AvroSchemaEncoder("test", time).build()
        self.assertEqual(expected, observed)

    def test_datetime(self):
        expected = {"name": "test", "type": {"type": "long", "logicalType": "timestamp-micros"}}
        observed = AvroSchemaEncoder("test", datetime).build()
        self.assertEqual(expected, observed)

    def test_dict(self):
        expected = {"name": "test", "type": {"type": "map", "values": "int"}}
        observed = AvroSchemaEncoder("test", dict[str, int]).build()
        self.assertEqual(expected, observed)

    def test_model_ref(self):
        expected = {
            "name": "test",
            "type": [
                {
                    "fields": [
                        {"name": "uuid", "type": {"logicalType": "uuid", "type": "string"}},
                        {"name": "version", "type": "int"},
                        {"name": "created_at", "type": {"logicalType": "timestamp-micros", "type": "long"}},
                        {"name": "updated_at", "type": {"logicalType": "timestamp-micros", "type": "long"}},
                        {"name": "name", "type": "string"},
                        {"name": "surname", "type": "string"},
                        {"name": "age", "type": ["int", "null"]},
                    ],
                    "name": "Owner",
                    "namespace": "tests.aggregate_classes.hello",
                    "type": "record",
                },
                {"type": "string", "logicalType": "uuid"},
                "null",
            ],
        }
        encoder = AvroSchemaEncoder("test", Optional[ModelRef[Owner]])
        encoder.generate_random_str = MagicMock(return_value="hello")

        observed = encoder.build()
        self.assertEqual(expected, observed)

    def test_list_model(self):
        expected = {
            "name": "test",
            "type": {
                "items": [
                    {
                        "fields": [{"name": "id", "type": "int"}, {"name": "username", "type": ["string", "null"]}],
                        "name": "User",
                        "namespace": "tests.model_classes.hello",
                        "type": "record",
                    },
                    "null",
                ],
                "type": "array",
            },
        }
        encoder = AvroSchemaEncoder("test", list[Optional[User]])
        encoder.generate_random_str = MagicMock(return_value="hello")

        observed = encoder.build()
        self.assertEqual(expected, observed)

    def test_uuid(self):
        expected = {"name": "test", "type": {"type": "string", "logicalType": "uuid"}}
        observed = AvroSchemaEncoder("test", UUID).build()
        self.assertEqual(expected, observed)

    def test_any(self):
        expected = {"name": "test", "type": "null"}
        observed = AvroSchemaEncoder("test", Any).build()
        self.assertEqual(expected, observed)


if __name__ == "__main__":
    unittest.main()
