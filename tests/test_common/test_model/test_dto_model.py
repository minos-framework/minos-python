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

    def test_avro_from_bytes_float(self):
        data = {"price": 120}
        schema = [
            {
                "fields": [{"name": "price", "type": "int"}],
                "name": "ShoppingList",
                "namespace": "tests.model_classes",
                "type": "record",
            },
        ]
        serialized = MinosAvroProtocol.encode(data, schema)
        self.assertEqual(True, isinstance(serialized, bytes))

        dto_model = DataTransferObject().from_avro_bytes(serialized)

        self.assertEqual(data["price"], dto_model.price)

    def test_avro_from_bytes_multiple_fields(self):
        data = {"cost": 3, "username": "test"}
        schema = [
            {
                "fields": [{"name": "cost", "type": "int"}, {"name": "username", "type": ["string", "null"]}],
                "name": "ShoppingList",
                "namespace": "tests.model_classes",
                "type": "record",
            },
        ]
        serialized = MinosAvroProtocol.encode(data, schema)
        self.assertEqual(True, isinstance(serialized, bytes))

        dto_model = DataTransferObject().from_avro_bytes(serialized)

        self.assertEqual(data["cost"], dto_model.cost)
        self.assertEqual(data["username"], dto_model.username)
