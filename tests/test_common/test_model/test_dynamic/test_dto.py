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
    MinosModelException,
    ModelField,
    ModelType,
)
from tests.model_classes import (
    Bar,
    Foo,
)


class TestDataTransferObject(unittest.IsolatedAsyncioTestCase):
    def test_from_avro_float(self):
        data = {"cost": 3.43}
        schema = [
            {"name": "ShoppingList", "fields": [{"name": "cost", "type": "float"}], "namespace": "", "type": "record"}
        ]

        dto = DataTransferObject.from_avro(schema, data)
        self.assertEqual(3.43, dto.cost)
        self.assertEqual(schema, dto.avro_schema)

    def test_from_avro_list(self):
        data = {"tickets": [3234, 3235, 3236]}
        schema = [
            {
                "fields": [{"name": "tickets", "type": {"items": "int", "type": "array"}}],
                "name": "ShoppingList",
                "namespace": "",
                "type": "record",
            }
        ]
        dto = DataTransferObject.from_avro(schema, data)

        self.assertEqual(data["tickets"], dto.tickets)
        self.assertEqual(schema, dto.avro_schema)

    def test_from_avro_dict(self):
        data = {"tickets": {"a": 1, "b": 2}}
        schema = [
            {
                "fields": [{"name": "tickets", "type": {"type": "map", "values": "int"}}],
                "name": "Order",
                "namespace": "",
                "type": "record",
            }
        ]
        dto = DataTransferObject.from_avro(schema, data)

        self.assertEqual(data["tickets"], dto.tickets)
        self.assertEqual(schema, dto.avro_schema)

    def test_from_avro_int(self):
        data = {"price": 120}
        schema = [{"fields": [{"name": "price", "type": "int"}], "name": "Order", "namespace": "", "type": "record"}]
        dto = DataTransferObject.from_avro(schema, data)

        self.assertEqual(data["price"], dto.price)

        self.assertEqual(schema, dto.avro_schema)

    def test_from_avro_union(self):
        data = {"username": "test"}
        schema = [
            {
                "fields": [{"name": "username", "type": ["string", "null"]}],
                "name": "Order",
                "namespace": "",
                "type": "record",
            }
        ]
        dto = DataTransferObject.from_avro(schema, data)

        self.assertEqual(data["username"], dto.username)
        self.assertEqual(schema, dto.avro_schema)

    def test_from_avro_dto(self):
        expected = DataTransferObject(
            "Order",
            fields={
                "price": ModelField("price", int, 34),
                "user": ModelField(
                    "user",
                    ModelType.build("User", {"username": list[int]}),
                    DataTransferObject(
                        "User", fields={"username": ModelField("username", list[int], [434324, 66464, 45432])}
                    ),
                ),
            },
        )

        data = {"price": 34, "user": {"username": [434324, 66464, 45432]}}
        schema = {
            "fields": [
                {
                    "name": "user",
                    "type": {
                        "fields": [{"name": "username", "type": {"type": "array", "items": "int"}}],
                        "name": "User",
                        "namespace": "",
                        "type": "record",
                    },
                },
                {"name": "price", "type": "int"},
            ],
            "name": "Order",
            "namespace": "",
            "type": "record",
        }
        dto = DataTransferObject.from_avro(schema, data)

        self.assertEqual(expected, dto)

    def test_from_avro_model(self):
        expected = Bar(first=Foo("one"), second=Foo("two"))
        dto = DataTransferObject.from_avro_bytes(expected.avro_bytes)
        self.assertEqual(expected, dto)

    def test_from_avro_with_namespace(self):
        schema = [
            {"fields": [{"name": "price", "type": "int"}], "name": "Order", "namespace": "example", "type": "record"}
        ]
        dto = DataTransferObject.from_avro(schema, {"price": 120})
        self.assertEqual(schema, dto.avro_schema)

    def test_classname(self):
        dto = DataTransferObject("Order", "example", {})
        self.assertEqual("example.Order", dto.classname)

    def test_from_typed_dict_model(self):
        dto = DataTransferObject.from_typed_dict(TypedDict("tests.model_classes.Foo", {"text": str}), {"text": "test"})
        self.assertEqual(Foo("test"), dto)

    def test_from_model_type_model(self):
        dto = DataTransferObject.from_model_type(
            ModelType.build("tests.model_classes.Foo", {"text": str}), {"text": "test"}
        )
        self.assertEqual(Foo("test"), dto)

    def test_from_typed_dict_model_raised(self):
        with self.assertRaises(MinosModelException):
            DataTransferObject.from_typed_dict(TypedDict("tests.model_classes.Foo", {"bar": str}), {"bar": "test"})

    def test_from_model_type_raised(self):
        with self.assertRaises(MinosModelException):
            DataTransferObject.from_model_type(
                ModelType.build("tests.model_classes.Foo", {"bar": str}), {"bar": "test"}
            )

    def test_avro_schema(self):
        data = {"price": 34, "user": {"username": [434324, 66464, 45432]}}
        schema = [
            {
                "fields": [
                    {
                        "name": "user",
                        "type": {
                            "fields": [{"name": "username", "type": {"type": "array", "items": "int"}}],
                            "name": "User",
                            "type": "record",
                            "namespace": "",
                        },
                    },
                    {"name": "price", "type": "int"},
                ],
                "name": "Order",
                "namespace": "",
                "type": "record",
            }
        ]
        dto = DataTransferObject.from_avro(schema, data)

        self.assertEqual(schema, dto.avro_schema)


if __name__ == "__main__":
    unittest.main()
