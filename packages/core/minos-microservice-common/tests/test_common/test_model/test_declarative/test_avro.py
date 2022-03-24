import unittest
from unittest.mock import (
    call,
    patch,
)
from uuid import (
    uuid4,
)

from minos.common import (
    EmptyMinosModelSequenceException,
    MissingSentinel,
    Model,
    MultiTypeMinosModelSequenceException,
)
from tests.model_classes import (
    Auth,
    Bar,
    Customer,
    Foo,
    FooBar,
    GenericUser,
    ShoppingList,
    User,
)


class TestModelAvro(unittest.IsolatedAsyncioTestCase):
    def test_avro_schema(self):
        expected = [
            {
                "fields": [
                    {
                        "name": "user",
                        "type": [
                            {
                                "fields": [
                                    {"name": "id", "type": "int"},
                                    {"name": "username", "type": ["string", "null"]},
                                ],
                                "name": "User",
                                "namespace": "tests.model_classes.goodbye",
                                "type": "record",
                            },
                            "null",
                        ],
                    },
                    {"name": "cost", "type": "double"},
                ],
                "name": "ShoppingList",
                "namespace": "tests.model_classes.hello",
                "type": "record",
            }
        ]
        with patch("minos.common.AvroSchemaEncoder.generate_random_str", side_effect=["hello", "goodbye"]):
            self.assertEqual(expected, ShoppingList.avro_schema)

    def test_avro_schema_generics(self):
        expected = [
            {
                "fields": [{"name": "username", "type": ["string", "int"]}],
                "name": "GenericUser",
                "namespace": "tests.model_classes.hello",
                "type": "record",
            }
        ]
        with patch("minos.common.AvroSchemaEncoder.generate_random_str", side_effect=["hello"]):
            self.assertEqual(expected, GenericUser.avro_schema)

    def test_avro_schema_generics_nested(self):
        expected = [
            {
                "fields": [
                    {
                        "name": "user",
                        "type": [
                            {
                                "fields": [{"name": "username", "type": "string"}],
                                "name": "GenericUser",
                                "namespace": "tests.model_classes.goodbye",
                                "type": "record",
                            }
                        ],
                    }
                ],
                "name": "Auth",
                "namespace": "tests.model_classes.hello",
                "type": "record",
            }
        ]
        with patch("minos.common.AvroSchemaEncoder.generate_random_str", side_effect=["hello", "goodbye"]):
            self.assertEqual(expected, Auth.avro_schema)

    def test_avro_schema_simple(self):
        customer = Customer(1234)
        expected = [
            {
                "fields": [
                    {"name": "id", "type": "int"},
                    {"name": "username", "type": ["string", "null"]},
                    {"name": "name", "type": ["string", "null"]},
                    {"name": "surname", "type": ["string", "null"]},
                    {"name": "is_admin", "type": ["boolean", "null"]},
                    {"name": "lists", "type": [{"items": "int", "type": "array"}, "null"]},
                ],
                "name": "Customer",
                "namespace": "tests.model_classes.hello",
                "type": "record",
            }
        ]
        with patch("minos.common.AvroSchemaEncoder.generate_random_str", side_effect=["hello"]):
            self.assertEqual(expected, customer.avro_schema)

    def test_avro_schema_multiple_fields(self):
        bar = Bar(first=Foo("one"), second=Foo("two"))
        expected = [
            {
                "fields": [
                    {
                        "name": "first",
                        "type": {
                            "fields": [{"name": "text", "type": "string"}],
                            "name": "Foo",
                            "namespace": "tests.model_classes.hello",
                            "type": "record",
                        },
                    },
                    {
                        "name": "second",
                        "type": {
                            "fields": [{"name": "text", "type": "string"}],
                            "name": "Foo",
                            "namespace": "tests.model_classes.goodbye",
                            "type": "record",
                        },
                    },
                ],
                "name": "Bar",
                "namespace": "tests.model_classes.one",
                "type": "record",
            }
        ]

        with patch("minos.common.AvroSchemaEncoder.generate_random_str", side_effect=["one", "hello", "goodbye"]):
            self.assertEqual(expected, bar.avro_schema)

    def test_encode_schema(self):
        user = User(1234)
        shopping_list = ShoppingList(user)
        with patch.object(ShoppingList, "encode_schema", return_value=MissingSentinel) as shopping_mock:
            with patch.object(User, "encode_schema", return_value=user.avro_schema) as user_mock:
                shopping_list.avro_schema

        encoder = shopping_mock.call_args_list[0].args[0]

        self.assertEqual([call(encoder, shopping_list.model_type)], shopping_mock.call_args_list)

        self.assertEqual([call(encoder, user.model_type)], user_mock.call_args_list)

    def test_avro_data(self):
        shopping_list = ShoppingList(User(1234))
        expected = {"cost": float("inf"), "user": {"id": 1234, "username": None}}
        self.assertEqual(expected, shopping_list.avro_data)

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

    def test_avro_data_multiple_fields(self):
        bar = Bar(first=Foo("one"), second=Foo("two"))
        expected = {"first": {"text": "one"}, "second": {"text": "two"}}

        self.assertEqual(expected, bar.avro_data)

    def test_encode_data(self):
        user = User(1234)
        shopping_list = ShoppingList(user)
        with patch.object(ShoppingList, "encode_data", return_value=MissingSentinel) as shopping_mock:
            with patch.object(User, "encode_data", return_value=user.avro_data) as user_mock:
                shopping_list.avro_data

        encoder = shopping_mock.call_args_list[0].args[0]

        self.assertEqual(
            [call(encoder, {"user": {"id": 1234, "username": None}, "cost": float("inf")})],
            shopping_mock.call_args_list,
        )

        self.assertEqual([call(encoder, {"id": 1234, "username": None})], user_mock.call_args_list)

    def test_avro_bytes(self):
        shopping_list = ShoppingList(User(1234))
        self.assertIsInstance(shopping_list.avro_bytes, bytes)

    def test_to_avro_bytes_sequence(self):
        customers = [Customer(1234), Customer(5678)]
        avro_bytes = Customer.to_avro_bytes(customers)
        self.assertIsInstance(avro_bytes, bytes)

    def test_to_avro_bytes_empty_sequence(self):
        with self.assertRaises(EmptyMinosModelSequenceException):
            Customer.to_avro_bytes([])

    def test_to_avro_bytes_multi_type_sequence(self):
        with self.assertRaises(MultiTypeMinosModelSequenceException):
            Customer.to_avro_bytes([User(1234), Customer(5678)])

    def test_avro_str_single(self):
        customer = Customer(1234)
        avro_str = customer.avro_str
        self.assertIsInstance(avro_str, str)

    def test_to_avro_str_sequence(self):
        customers = [Customer(1234), Customer(5678)]
        avro_str = Customer.to_avro_str(customers)
        self.assertIsInstance(avro_str, str)

    def test_decode_schema(self):
        user = User(1234)
        shopping_list = ShoppingList(user)

        with patch("minos.common.AvroSchemaEncoder.generate_random_str", return_value="hello"):
            with patch.object(Model, "decode_schema", side_effect=[MissingSentinel, User]) as mock:
                # noinspection PyTypeChecker
                Model.from_avro(shopping_list.avro_schema, shopping_list.avro_data)

            decoder = mock.call_args_list[0].args[0]

            self.assertEqual(
                [call(decoder, shopping_list.avro_schema[0]), call(decoder, user.avro_schema[0])], mock.call_args_list
            )

    def test_decode_data(self):
        user = User(1234)
        shopping_list = ShoppingList(user)

        with patch.object(Model, "decode_data", side_effect=[MissingSentinel, user]) as mock:
            # noinspection PyTypeChecker
            Model.from_avro(shopping_list.avro_schema, shopping_list.avro_data)

        decoder = mock.call_args_list[0].args[0]

        self.assertEqual(
            [
                call(decoder, {"user": {"id": 1234, "username": None}, "cost": float("inf")}, shopping_list.model_type),
                call(decoder, {"id": 1234, "username": None}, user.model_type),
            ],
            mock.call_args_list,
        )

    def test_from_avro_bytes_single(self):
        customer = Customer(1234)
        avro_bytes = customer.avro_bytes
        self.assertIsInstance(avro_bytes, bytes)
        decoded_customer = Customer.from_avro_bytes(avro_bytes)
        self.assertEqual(customer, decoded_customer)

    def test_from_avro_bytes_in_batch(self):
        customers = [Customer(1234), Customer(5678)]
        avro_bytes = Customer.to_avro_bytes(customers)
        self.assertIsInstance(avro_bytes, bytes)
        decoded_customer = Customer.from_avro_bytes(avro_bytes, batch_mode=True)
        self.assertEqual(customers, decoded_customer)

    def test_from_avro_bytes_composed(self):
        shopping_list = ShoppingList(User(1234), cost="1.234")
        avro_bytes = shopping_list.avro_bytes
        decoded_shopping_list = ShoppingList.from_avro_bytes(avro_bytes)
        self.assertEqual(shopping_list, decoded_shopping_list)

    def test_from_avro_bytes_multiple_fields(self):
        original = Bar(first=Foo("one"), second=Foo("two"))
        serialized = original.avro_bytes
        recovered = Bar.from_avro_bytes(serialized)
        self.assertEqual(original, recovered)

    def test_from_avro_bytes_uuid(self):
        original = FooBar(uuid4())
        serialized = original.avro_bytes
        recovered = FooBar.from_avro_bytes(serialized)
        self.assertEqual(original, recovered)

    def test_from_avro_bytes_generics(self):
        base = GenericUser("foo")
        self.assertEqual(base, GenericUser.from_avro_bytes(base.avro_bytes))

    def test_from_avro_bytes_generics_nested(self):
        base = Auth(GenericUser("foo"))
        self.assertEqual(base, Auth.from_avro_bytes(base.avro_bytes))

    def test_from_avro_str_single(self):
        customer = Customer(1234)
        avro_str = customer.avro_str

        decoded_customer = Customer.from_avro_str(avro_str)
        self.assertEqual(customer, decoded_customer)

    def test_from_avro_str_in_batch(self):
        customers = [Customer(1234), Customer(5678)]
        avro_str = Customer.to_avro_str(customers)

        decoded_customer = Customer.from_avro_str(avro_str, batch_mode=True)
        self.assertEqual(customers, decoded_customer)


if __name__ == "__main__":
    unittest.main()
