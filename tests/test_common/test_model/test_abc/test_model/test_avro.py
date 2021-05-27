"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import unittest

from minos.common import (
    EmptyMinosModelSequenceException,
    MultiTypeMinosModelSequenceException,
)
from tests.aggregate_classes import (
    Car,
    Owner,
)
from tests.model_classes import (
    Bar,
    Customer,
    Foo,
    ShoppingList,
    User,
)


class TestMinosModelAvro(unittest.TestCase):
    def test_avro_schema(self):
        expected = {
            "fields": [
                {
                    "name": "user",
                    "type": [
                        {
                            "fields": [{"name": "id", "type": "int"}, {"name": "username", "type": ["string", "null"]}],
                            "name": "User",
                            "namespace": "tests.model_classes",
                            "type": "record",
                        },
                        "null",
                    ],
                },
                {"name": "cost", "type": "float"},
            ],
            "name": "ShoppingList",
            "namespace": "tests.model_classes",
            "type": "record",
        }
        self.assertEqual(expected, ShoppingList.avro_schema)

    def test_avro_data(self):
        shopping_list = ShoppingList(User(1234))
        expected = {"cost": float("inf"), "user": {"id": 1234, "username": None}}
        self.assertEqual(expected, shopping_list.avro_data)

    def test_avro_bytes(self):
        shopping_list = ShoppingList(User(1234))
        self.assertIsInstance(shopping_list.avro_bytes, bytes)

    def test_avro_schema_model_ref(self):
        # noinspection DuplicatedCode
        expected = {
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "version", "type": "int"},
                {"name": "doors", "type": "int"},
                {"name": "color", "type": "string"},
                {
                    "name": "owner",
                    "type": [
                        {
                            "default": [],
                            "items": [
                                {
                                    "fields": [
                                        {"name": "id", "type": "int"},
                                        {"name": "version", "type": "int"},
                                        {"name": "name", "type": "string"},
                                        {"name": "surname", "type": "string"},
                                        {"name": "age", "type": ["int", "null"]},
                                    ],
                                    "name": "Owner",
                                    "namespace": "tests.aggregate_classes",
                                    "type": "record",
                                },
                                "int",
                            ],
                            "type": "array",
                        },
                        "null",
                    ],
                },
            ],
            "name": "Car",
            "namespace": "tests.aggregate_classes",
            "type": "record",
        }
        self.assertEqual(expected, Car.avro_schema)

    def test_avro_data_model_ref(self):
        car = Car(1, 1, 5, "blue", [Owner(1, 1, "Hello", "Good Bye"), Owner(2, 1, "Foo", "Bar")])
        expected = {
            "color": "blue",
            "doors": 5,
            "id": 1,
            "owner": [
                {"age": None, "id": 1, "name": "Hello", "surname": "Good Bye", "version": 1},
                {"age": None, "id": 2, "name": "Foo", "surname": "Bar", "version": 1},
            ],
            "version": 1,
        }
        self.assertEqual(expected, car.avro_data)

    def test_avro_bytes_model_ref(self):
        car = Car(1, 1, 5, "blue", [Owner(1, 1, "Hello", "Good Bye"), Owner(2, 1, "Foo", "Bar")])
        self.assertIsInstance(car.avro_bytes, bytes)

    def test_avro_schema_simple(self):
        customer = Customer(1234)
        expected = {
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "username", "type": ["string", "null"]},
                {"name": "name", "type": ["string", "null"]},
                {"name": "surname", "type": ["string", "null"]},
                {"name": "is_admin", "type": ["boolean", "null"]},
                {"name": "lists", "type": [{"default": [], "items": "int", "type": "array"}, "null"]},
            ],
            "name": "Customer",
            "namespace": "tests.model_classes",
            "type": "record",
        }
        self.assertEqual(expected, customer.avro_schema)

    def test_avro_data_simple(self):
        customer = Customer(1234)
        expected = {
            "id": 1234,
            "is_admin": None,
            "lists": None,
            "name": None,
            "surname": None,
            "username": None,
        }
        self.assertEqual(expected, customer.avro_data)

    def test_avro_avro_str_single(self):
        customer = Customer(1234)
        avro_str = customer.avro_str
        self.assertIsInstance(avro_str, str)
        decoded_customer = Customer.from_avro_str(avro_str)
        self.assertEqual(customer, decoded_customer)

    def test_avro_bytes_single(self):
        customer = Customer(1234)
        avro_bytes = customer.avro_bytes
        self.assertIsInstance(avro_bytes, bytes)
        decoded_customer = Customer.from_avro_bytes(avro_bytes)
        self.assertEqual(customer, decoded_customer)

    def test_avro_to_avro_str(self):
        customers = [Customer(1234), Customer(5678)]
        avro_str = Customer.to_avro_str(customers)
        self.assertIsInstance(avro_str, str)
        decoded_customer = Customer.from_avro_str(avro_str)
        self.assertEqual(customers, decoded_customer)

    def test_avro_bytes_sequence(self):
        customers = [Customer(1234), Customer(5678)]
        avro_bytes = Customer.to_avro_bytes(customers)
        self.assertIsInstance(avro_bytes, bytes)
        decoded_customer = Customer.from_avro_bytes(avro_bytes)
        self.assertEqual(customers, decoded_customer)

    def test_avro_bytes_composed(self):
        shopping_list = ShoppingList(User(1234), cost="1.234")
        avro_bytes = shopping_list.avro_bytes
        self.assertIsInstance(avro_bytes, bytes)
        decoded_shopping_list = ShoppingList.from_avro_bytes(avro_bytes)
        self.assertEqual(shopping_list, decoded_shopping_list)

    def test_avro_bytes_empty_sequence(self):
        with self.assertRaises(EmptyMinosModelSequenceException):
            Customer.to_avro_bytes([])

    def test_avro_bytes_multi_type_sequence(self):
        with self.assertRaises(MultiTypeMinosModelSequenceException):
            Customer.to_avro_bytes([User(1234), Customer(5678)])

    @unittest.skip
    def test_multiple_fields_avro_schema(self):
        bar = Bar(first=Foo("one"), second=Foo("two"))
        expected = {
            # TODO
        }

        self.assertEqual(expected, bar.avro_schema)

    def test_multiple_fields_avro_data(self):
        bar = Bar(first=Foo("one"), second=Foo("two"))
        expected = {"first": {"text": "one"}, "second": {"text": "two"}}

        self.assertEqual(expected, bar.avro_data)

    @unittest.skip
    def test_multiple_fields_avro_bytes(self):
        original = Bar(first=Foo("one"), second=Foo("two"))
        serialized = original.avro_bytes
        recovered = Bar.from_avro_bytes(serialized)
        self.assertEqual(original, recovered)


if __name__ == "__main__":
    unittest.main()
