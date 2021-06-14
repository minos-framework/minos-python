"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import unittest
from typing import (
    TypedDict,
)

from minos.common import (
    DataTransferObject,
    MinosAvroProtocol,
    ModelField,
)
from tests.model_classes import (
    Bar,
    Foo,
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
            "fields": [{"name": "tickets", "type": "array", "items": "int"}],
        }
        dto = DataTransferObject.from_avro(schema, data)

        self.assertEqual(data["tickets"], dto.tickets)

    def test_build_map(self):
        data = {"tickets": {"a": 1, "b": 2}}
        schema = {
            "name": "Order",
            "fields": [{"name": "tickets", "type": {"type": "map", "values": "int"}}],
        }
        dto = DataTransferObject.from_avro(schema, data)

        self.assertEqual(data["tickets"], dto.tickets)

    def test_from_avro_int(self):
        data = {"price": 120}
        schema = [
            {"fields": [{"name": "price", "type": "int"}], "name": "Order", "type": "record"},
        ]
        dto = DataTransferObject.from_avro(schema, data)

        self.assertEqual(data["price"], dto.price)

        self.assertEqual(schema, dto.avro_schema)

    def test_from_avro_multiple_fields(self):
        data = {"cost": 3, "username": "test", "tickets": [3234, 3235, 3236]}
        schema = [
            {
                "fields": [{"name": "cost", "type": "int"}, {"name": "username", "type": ["string", "null"]}],
                "name": "Order",
                "type": "record",
            },
        ]
        dto = DataTransferObject.from_avro(schema, data)

        self.assertEqual(data["cost"], dto.cost)
        self.assertEqual(data["username"], dto.username)

        self.assertEqual(schema, dto.avro_schema)

    def test_from_avro_array(self):
        data = {"tickets": [3234, 3235, 3236]}
        schema = [
            {
                "fields": [{"name": "tickets", "type": {"type": "array", "items": "int"}}],
                "name": "Order",
                "type": "record",
            },
        ]
        dto = DataTransferObject.from_avro(schema, data)

        self.assertEqual(data["tickets"], dto.tickets)

        self.assertEqual(schema, dto.avro_schema)

    def test_from_avro_map(self):
        data = {"tickets": {"a": 1, "b": 2}}
        schema = [
            {
                "fields": [{"name": "tickets", "type": {"type": "map", "values": "int"}}],
                "name": "Order",
                "type": "record",
            },
        ]
        dto = DataTransferObject.from_avro(schema, data)

        self.assertEqual(data["tickets"], dto.tickets)

        self.assertEqual(schema, dto.avro_schema)

    def test_from_avro_multi_schema(self):
        data = {"price": 34, "user": {"username": [434324, 66464, 45432]}}
        schema = [
            {
                "fields": [{"name": "username", "type": {"type": "array", "items": "int"}}],
                "name": "User",
                "type": "record",
            },
            {
                "fields": [{"name": "user", "type": "User"}, {"name": "price", "type": "int"}],
                "name": "Order",
                "type": "record",
            },
        ]

        serialized = MinosAvroProtocol.encode(data, schema)
        self.assertEqual(True, isinstance(serialized, bytes))

        dto = DataTransferObject.from_avro_bytes(serialized)

        expected = DataTransferObject(
            "Order",
            fields={
                "price": ModelField("price", int, 34),
                "user": ModelField(
                    "user",
                    TypedDict("User", {"username": list[int]}),
                    DataTransferObject(
                        "User", fields={"username": ModelField("username", list[int], [434324, 66464, 45432])}
                    ),
                ),
            },
        )

        self.assertEqual(expected.price, dto.price)
        self.assertEqual(expected.user, dto.user)

    def test_multiple_fields_avro_schema(self):
        expected = Bar(first=Foo("one"), second=Foo("two"))
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
        self.assertEqual(expected, dto)


if __name__ == "__main__":
    unittest.main()
