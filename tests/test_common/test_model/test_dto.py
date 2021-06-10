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
        schema = {"name": "cost", "type": "float"}

        dto_model = DataTransferObject()
        dto_model.build_field(schema, data["cost"])

        self.assertEqual(3.43, dto_model.cost)

    def test_build_array(self):
        data = {"tickets": [3234, 3235, 3236]}
        schema = {"name": "tickets", "type": "array", "items": "int"}

        dto_model = DataTransferObject()
        dto_model.build_field(schema, data["tickets"])

        self.assertEqual(data["tickets"], dto_model.tickets)

    def test_build_map(self):
        data = {"tickets": {"a": 1, "b": 2}}
        schema = {"name": "tickets", "type": {"type": "map", "values": "int"}}

        dto_model = DataTransferObject()
        dto_model.build_field(schema, data["tickets"])

        self.assertEqual(data["tickets"], dto_model.tickets)

    def test_avro_from_bytes_int(self):
        data = {"price": 120}
        schema = [
            {
                "fields": [{"name": "price", "type": "int"}],
                "name": "tests.model_classes.ShoppingList",
                "type": "record",
            },
        ]
        serialized = MinosAvroProtocol.encode(data, schema)
        self.assertEqual(True, isinstance(serialized, bytes))

        dto_model = DataTransferObject.from_avro_bytes(serialized)

        self.assertEqual(data["price"], dto_model.price)

        decoded_schema = MinosAvroProtocol.decode_schema(serialized)
        self.assertEqual(schema[0], decoded_schema)

    def test_avro_from_bytes_multiple_fields(self):
        data = {"cost": 3, "username": "test", "tickets": [3234, 3235, 3236]}
        schema = [
            {
                "fields": [{"name": "cost", "type": "int"}, {"name": "username", "type": ["string", "null"]}],
                "name": "tests.model_classes.ShoppingList",
                "type": "record",
            },
        ]
        serialized = MinosAvroProtocol.encode(data, schema)
        self.assertEqual(True, isinstance(serialized, bytes))

        dto_model = DataTransferObject.from_avro_bytes(serialized)

        self.assertEqual(data["cost"], dto_model.cost)
        self.assertEqual(data["username"], dto_model.username)

        decoded_schema = MinosAvroProtocol.decode_schema(serialized)
        self.assertEqual(schema[0], decoded_schema)

    def test_avro_from_bytes_array(self):
        data = {"tickets": [3234, 3235, 3236]}
        schema = [
            {
                "fields": [{"name": "tickets", "type": {"type": "array", "items": "int"}}],
                "name": "tests.model_classes.ShoppingList",
                "type": "record",
            },
        ]
        serialized = MinosAvroProtocol.encode(data, schema)
        self.assertEqual(True, isinstance(serialized, bytes))

        dto_model = DataTransferObject.from_avro_bytes(serialized)

        self.assertEqual(data["tickets"], dto_model.tickets)

        decoded_schema = MinosAvroProtocol.decode_schema(serialized)
        self.assertEqual(schema[0], decoded_schema)

    def test_avro_from_bytes_map(self):
        data = {"tickets": {"a": 1, "b": 2}}
        schema = [
            {
                "fields": [{"name": "tickets", "type": {"type": "map", "values": "int"}}],
                "name": "tests.model_classes.ShoppingList",
                "type": "record",
            },
        ]
        serialized = MinosAvroProtocol.encode(data, schema)
        self.assertEqual(True, isinstance(serialized, bytes))

        dto_model = DataTransferObject.from_avro_bytes(serialized)

        self.assertEqual(data["tickets"], dto_model.tickets)

        decoded_schema = MinosAvroProtocol.decode_schema(serialized)
        self.assertEqual(schema[0], decoded_schema)

    def test_avro_from_bytes_multi_schema(self):
        data = {"price": 34, "user": {"username": [434324, 66464, 45432]}}
        schema = [
            {
                "fields": [{"name": "username", "type": {"type": "array", "items": "int"}}],
                "name": "User",
                "namespace": "tests.model_classes",
                "type": "record",
            },
            {
                "fields": [{"name": "user", "type": "User"}, {"name": "price", "type": "int"}],
                "name": "ShoppingList",
                "namespace": "tests.model_classes",
                "type": "record",
            },
        ]

        serialized = MinosAvroProtocol.encode(data, schema)
        self.assertEqual(True, isinstance(serialized, bytes))

        dto_model = DataTransferObject.from_avro_bytes(serialized)

        self.assertEqual(data["price"], dto_model.price)
        # self.assertEqual(data["user"], dto_model.user)

        """
        {
            "type": "record",
            "name": "tests.model_classes.ShoppingList",
            "fields": [
                {
                    "name": "user",
                    "type": {
                        "type": "record",
                        "name": "tests.model_classes.User",
                        "fields": [
                            {"name": "username", "type": {"type": "array", "items": "int"}}
                        ],
                    },
                },
                {"name": "price", "type": "int"},
            ],
        }
        """
