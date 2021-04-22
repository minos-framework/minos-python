"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import unittest
from typing import (
    Optional,
)

import pytest
from minos.common import (
    EmptyMinosModelSequenceException,
    MinosAttributeValidationException,
    MinosMalformedAttributeException,
    MinosModelException,
    MinosParseAttributeException,
    MinosReqAttributeException,
    MinosTypeAttributeException,
    ModelField,
    MultiTypeMinosModelSequenceException,
)
from tests.modelClasses import (
    Analytics,
    Customer,
    CustomerFailDict,
    CustomerFailList,
    ShoppingList,
    User,
)


class TestMinosModel(unittest.TestCase):
    def test_constructor_args(self):
        model = Customer(1234, "johndoe", "John", "Doe")
        self.assertEqual(1234, model.id)
        self.assertEqual("johndoe", model.username)
        self.assertEqual("John", model.name)
        self.assertEqual("Doe", model.surname)

    def test_constructor_multiple_values(self):
        with self.assertRaises(TypeError):
            Customer(1234, id=1234)
        with self.assertRaises(TypeError):
            Customer(None, id=1234)

    def test_constructor_kwargs(self):
        model = Customer(id=1234, username="johndoe", name="John", surname="Doe")
        self.assertEqual(1234, model.id)
        self.assertEqual("johndoe", model.username)
        self.assertEqual("Doe", model.surname)
        self.assertEqual("John", model.name)

    def test_aggregate_setter(self):
        model = Customer(1234)
        model.name = "John"
        model.surname = "Doe"
        self.assertEqual(1234, model.id)
        self.assertEqual("Doe", model.surname)
        self.assertEqual("John", model.name)

    def test_attribute_parser_same_type(self):
        model = Customer(1234, name="john")
        self.assertEqual("John", model.name)

    def test_attribute_parser_different_type(self):
        model = ShoppingList(User(1234), cost="1.234,56")
        self.assertEqual(1234.56, model.cost)

    def test_attribute_parser_attribute(self):
        model = ShoppingList(User(1234, "admin"), cost="1.234,56")
        self.assertEqual(0.0, model.cost)

    def test_aggregate_int_as_string_type_setter(self):
        model = Customer("1234")
        model.name = "John"
        self.assertEqual(1234, model.id)
        self.assertEqual("John", model.name)

    def test_aggregate_wrong_int_type_setter(self):
        with pytest.raises(MinosTypeAttributeException):
            Customer("1234S")

    def test_aggregate_string_type_setter(self):
        model = Customer(123)
        model.name = "John"
        self.assertEqual("John", model.name)

    def test_aggregate_wrong_string_type_setter_with_parser(self):
        model = Customer(123)
        with pytest.raises(MinosParseAttributeException):
            model.name = 456

    def test_aggregate_wrong_string_type_setter(self):
        model = Customer(123)
        with pytest.raises(MinosTypeAttributeException):
            model.surname = 456

    def test_aggregate_bool_type_setter(self):
        model = Customer(123)
        model.name = "John"
        model.is_admin = True
        self.assertTrue(model.is_admin)

    def test_aggregate_wrong_bool_type_setter(self):
        model = Customer(123)
        model.name = "John"
        with pytest.raises(MinosTypeAttributeException):
            model.is_admin = "True"

    def test_aggregate_empty_mandatory_field(self):
        with pytest.raises(MinosReqAttributeException):
            Customer()

    def test_model_is_freezed_class(self):
        model = Customer(123)
        with pytest.raises(MinosModelException):
            model.address = "str kennedy"

    def test_model_list_class_attribute(self):
        model = Customer(123)
        model.lists = [1, 5, 8, 6]

        self.assertEqual([1, 5, 8, 6], model.lists)

    def test_model_list_wrong_attribute_type(self):
        model = Customer(123)
        with pytest.raises(MinosTypeAttributeException):
            model.lists = [1, "hola", 8, 6]

    def test_model_ref(self):
        shopping_list = ShoppingList(cost=3.14)
        user = User(1234)
        shopping_list.user = user
        self.assertEqual(user, shopping_list.user)

    def test_avro_schema(self):
        shopping_list = ShoppingList(User(1234))
        expected = {
            "fields": [
                {
                    "name": "user",
                    "type": [
                        {
                            "fields": [{"name": "id", "type": "int"}, {"name": "username", "type": ["string", "null"]}],
                            "name": "User",
                            "namespace": "tests.modelClasses",
                            "type": "record",
                        },
                        "null",
                    ],
                },
                {"name": "cost", "type": "float"},
            ],
            "name": "ShoppingList",
            "namespace": "tests.modelClasses",
            "type": "record",
        }
        self.assertEqual(expected, shopping_list.avro_schema)

    def test_avro_data(self):
        shopping_list = ShoppingList(User(1234))
        expected = {"cost": float("inf"), "user": {"id": 1234, "username": None}}
        self.assertEqual(expected, shopping_list.avro_data)

    def test_avro_bytes(self):
        shopping_list = ShoppingList(User(1234))
        self.assertIsInstance(shopping_list.avro_bytes, bytes)

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
            "namespace": "tests.modelClasses",
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

    def test_avro_bytes_single(self):
        customer = Customer(1234)
        avro_bytes = customer.avro_bytes
        self.assertIsInstance(avro_bytes, bytes)
        decoded_customer = Customer.from_avro_bytes(avro_bytes)
        self.assertEqual(customer, decoded_customer)

    def test_avro_bytes_sequence(self):
        customers = [Customer(1234), Customer(5678)]
        avro_bytes = Customer.to_avro_bytes(customers)
        self.assertIsInstance(avro_bytes, bytes)
        decoded_customer = Customer.from_avro_bytes(avro_bytes)
        self.assertEqual(customers, decoded_customer)

    def test_avro_bytes_empty_sequence(self):
        with self.assertRaises(EmptyMinosModelSequenceException):
            Customer.to_avro_bytes([])

    def test_avro_bytes_multi_type_sequence(self):
        with self.assertRaises(MultiTypeMinosModelSequenceException):
            Customer.to_avro_bytes([User(1234), Customer(5678)])

    def test_model_ref_raises(self):
        shopping_list = ShoppingList(cost=3.14)
        with self.assertRaises(MinosTypeAttributeException):
            shopping_list.user = "foo"

    def test_recursive_type_composition(self):
        orders = {
            "foo": [ShoppingList(User(1)), ShoppingList(User(1))],
            "bar": [ShoppingList(User(2)), ShoppingList(User(2))],
        }

        analytics = Analytics(1, orders)
        self.assertEqual(orders, analytics.orders)

    def test_model_fail_list_class_attribute(self):
        with pytest.raises(MinosMalformedAttributeException):
            CustomerFailList(123)

    def test_model_fail_dict_class_attribute(self):
        with pytest.raises(MinosMalformedAttributeException):
            CustomerFailDict(123)

    def test_empty_required_value(self):
        with self.assertRaises(MinosReqAttributeException):
            User()

    def test_validate_required_raises(self):
        with self.assertRaises(MinosAttributeValidationException):
            User(-34)

    def test_validate_optional_raises(self):
        with self.assertRaises(MinosAttributeValidationException):
            User(1234, username="user name")

    def test_fields(self):
        user = User(123)
        fields = {
            "id": ModelField("id", int, 123, validator=user.validate_id),
            "username": ModelField(
                "username", Optional[str], parser=user.parse_username, validator=user.validate_username
            ),
        }
        self.assertEqual(fields, user.fields)

    def test_equal(self):
        a, b = User(123), User(123)
        self.assertEqual(a, b)

    def test_complex_equal(self):
        a, b = ShoppingList(User(1234), cost="1.234,56"), ShoppingList(User(1234), cost="1.234,56")
        self.assertEqual(a, b)

    def test_not_equal(self):
        a, b = User(123), User(456)
        self.assertNotEqual(a, b)

    def test_iter(self):
        user = User(123)
        expected = {
            "id": ModelField("id", int, 123, validator=user.validate_id),
            "username": ModelField(
                "username", Optional[str], parser=user.parse_username, validator=user.validate_username
            ),
        }
        self.assertEqual(expected, dict(user))

    def test_hash(self):
        user = User(123)

        expected = hash(
            (
                ("id", ModelField("id", int, 123, validator=user.validate_id)),
                (
                    "username",
                    ModelField("username", Optional[str], parser=user.parse_username, validator=user.validate_username),
                ),
            )
        )
        self.assertEqual(expected, hash(user))

    def test_repr(self):
        shopping_list = ShoppingList(User(1234), cost="1.234,56")
        expected = (
            "ShoppingList(fields=[ModelField(name='user', type=typing.Optional[minos.common.model.abc.types.ModelRef"
            "[tests.modelClasses.User]], value=User(fields=[ModelField(name='id', type=<class 'int'>, value=1234, "
            "parser=None, validator=validate_id), ModelField(name='username', type=typing.Optional[str], value=None, "
            "parser=parse_username, validator=validate_username)]), parser=None, validator=None), ModelField(name="
            "'cost', type=<class 'float'>, value=1234.56, parser=parse_cost, validator=None)])"
        )
        self.assertEqual(expected, repr(shopping_list))

    def test_classname_cls(self):
        self.assertEqual("tests.modelClasses.Customer", Customer.classname())

    def test_classname_instance(self):
        model = Customer(1234, "johndoe", "John", "Doe")
        self.assertEqual("tests.modelClasses.Customer", model.classname())


if __name__ == "__main__":
    unittest.main()
