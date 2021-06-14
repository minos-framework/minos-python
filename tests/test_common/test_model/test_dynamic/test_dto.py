"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import unittest

from minos.common import (
    DataTransferObject,
    MinosAvroProtocol,
)


class TestDataTransferObject(unittest.IsolatedAsyncioTestCase):
    def test_build_field(self):
        data = {"cost": 3.43}
        schema = {
            "name": "ShoppingList",
            "namespace": "tests.model_classes",
            "fields": [{"name": "cost", "type": "float"}],
        }

        dto = DataTransferObject.from_avro(schema, data)
        self.assertEqual(3.43, dto.cost)

    def test_build_array(self):
        data = {"tickets": [3234, 3235, 3236]}
        schema = {
            "name": "ShoppingList",
            "namespace": "tests.model_classes",
            "fields": [{"name": "tickets", "type": "array", "items": "int"}],
        }
        dto = DataTransferObject.from_avro(schema, data)

        self.assertEqual(data["tickets"], dto.tickets)

    def test_build_map(self):
        data = {"tickets": {"a": 1, "b": 2}}
        schema = {
            "name": "ShoppingList",
            "namespace": "tests.model_classes",
            "fields": [{"name": "tickets", "type": {"type": "map", "values": "int"}}],
        }
        dto = DataTransferObject.from_avro(schema, data)

        self.assertEqual(data["tickets"], dto.tickets)

    def test_avro_from_bytes_int(self):
        data = {"price": 120}
        schema = [
            {
                "fields": [{"name": "price", "type": "int"}],
                "name": "ShoppingList",
                "namespace": "tests.model_classes",
                "type": "record",
            },
        ]
        dto = DataTransferObject.from_avro(schema, data)

        self.assertEqual(data["price"], dto.price)

        self.assertEqual(schema, dto.avro_schema)

    def test_avro_from_bytes_multiple_fields(self):
        data = {"cost": 3, "username": "test", "tickets": [3234, 3235, 3236]}
        schema = [
            {
                "fields": [{"name": "cost", "type": "int"}, {"name": "username", "type": ["string", "null"]}],
                "name": "ShoppingList",
                "namespace": "tests.model_classes",
                "type": "record",
            },
        ]
        dto = DataTransferObject.from_avro(schema, data)

        self.assertEqual(data["cost"], dto.cost)
        self.assertEqual(data["username"], dto.username)

        self.assertEqual(schema, dto.avro_schema)

    def test_avro_from_bytes_array(self):
        data = {"tickets": [3234, 3235, 3236]}
        schema = [
            {
                "fields": [{"name": "tickets", "type": {"type": "array", "items": "int"}}],
                "name": "ShoppingList",
                "namespace": "tests.model_classes",
                "type": "record",
            },
        ]
        dto = DataTransferObject.from_avro(schema, data)

        self.assertEqual(data["tickets"], dto.tickets)

        self.assertEqual(schema, dto.avro_schema)

    def test_avro_from_bytes_map(self):
        data = {"tickets": {"a": 1, "b": 2}}
        schema = [
            {
                "fields": [{"name": "tickets", "type": {"type": "map", "values": "int"}}],
                "name": "ShoppingList",
                "namespace": "tests.model_classes",
                "type": "record",
            },
        ]
        dto = DataTransferObject.from_avro(schema, data)

        self.assertEqual(data["tickets"], dto.tickets)

        self.assertEqual(schema, dto.avro_schema)

    def test_avro_from_bytes_multi_schema(self):
        data = {"price": 34, "user": {"username": [434324, 66464, 45432]}}
        schema = [
            {
                "fields": [{"name": "username", "type": {"type": "array", "items": "int"}}],
                "name": "UserDTO",
                "namespace": "tests.model_classes",
                "type": "record",
            },
            {
                "fields": [{"name": "user", "type": "UserDTO"}, {"name": "price", "type": "int"}],
                "name": "ShoppingListDTO",
                "namespace": "tests.model_classes",
                "type": "record",
            },
        ]

        serialized = MinosAvroProtocol.encode(data, schema)
        self.assertEqual(True, isinstance(serialized, bytes))

        dto = DataTransferObject.from_avro_bytes(serialized)

        self.assertEqual(data["price"], dto.price)
        self.assertIsInstance(dto.user, DataTransferObject)
        self.assertEqual(data["user"], dto.user.avro_data)

    def test_multiple_fields_avro_schema(self):
        data = {"first": {"text": "one"}, "second": {"text": "two"}}
        schema = [
            {
                "fields": [
                    {
                        "name": "first",
                        "type": [
                            {
                                "fields": [{"name": "text", "type": "string"}],
                                "name": "Foo",
                                "namespace": "tests.model_classes.first",
                                "type": "record",
                            }
                        ],
                    },
                    {
                        "name": "second",
                        "type": [
                            {
                                "fields": [{"name": "text", "type": "string"}],
                                "name": "Foo",
                                "namespace": "tests.model_classes.second",
                                "type": "record",
                            }
                        ],
                    },
                ],
                "name": "Bar",
                "namespace": "tests.model_classes",
                "type": "record",
            }
        ]
        dto = DataTransferObject.from_avro(schema, data)
        self.assertEqual(schema, dto.avro_schema)


if __name__ == "__main__":
    unittest.main()
