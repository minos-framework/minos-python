import unittest

from minos.common import (
    MinosAvroProtocol,
    MinosProtocolException,
)


class TestMinosAvroProtocol(unittest.TestCase):
    def test_multi_schema(self):
        data = {"cost": float("inf"), "user": {"id": 1234, "username": None}}
        schema = [
            {
                "fields": [{"name": "user", "type": ["User", "null"]}, {"name": "cost", "type": "float"}],
                "name": "ShoppingList",
                "namespace": "tests.model_classes",
                "type": "record",
            },
            {
                "fields": [{"name": "id", "type": "int"}, {"name": "username", "type": ["string", "null"]}],
                "name": "User",
                "namespace": "tests.model_classes",
                "type": "record",
            },
        ]
        serialized = MinosAvroProtocol.encode(data, schema)
        self.assertEqual(True, isinstance(serialized, bytes))

        # decode
        deserialized = MinosAvroProtocol.decode(serialized)
        self.assertEqual(data, deserialized)

        expected_schema = {
            "type": "record",
            "name": "tests.model_classes.ShoppingList",
            "fields": [
                {
                    "name": "user",
                    "type": [
                        {
                            "type": "record",
                            "name": "tests.model_classes.User",
                            "fields": [
                                {"name": "id", "type": "int"},
                                {"name": "username", "type": ["string", "null"]},
                            ],
                        },
                        "null",
                    ],
                },
                {"name": "cost", "type": "float"},
            ],
        }

        decoded_schema = MinosAvroProtocol.decode_schema(serialized)
        self.assertEqual(decoded_schema, expected_schema)

    def test_decode_schema_raise_exception(self):
        data = b"Test"

        with self.assertRaises(Exception) as context:
            MinosAvroProtocol.decode_schema(data)

        self.assertTrue("Error getting avro schema" in str(context.exception))

    def test_float(self):
        schema = {
            "type": "record",
            "name": "tests.model_classes.ShoppingList",
            "fields": [{"type": "double", "name": "foo"}],
        }
        data = {"foo": 3.14159265359}
        serialized = MinosAvroProtocol.encode(data, schema)
        deserialized = MinosAvroProtocol.decode(serialized)
        self.assertEqual(data, deserialized)

    def test_timedelta(self):
        schema = {
            "type": "record",
            "name": "tests.model_classes.ShoppingList",
            "fields": [{"type": "long", "name": "foo", "logicalType": "timedelta-micros"}],
        }
        data = {"foo": 2030401000023}
        serialized = MinosAvroProtocol.encode(data, schema)
        deserialized = MinosAvroProtocol.decode(serialized)
        self.assertEqual(data, deserialized)

    def test_set(self):
        schema = {
            "type": "record",
            "name": "tests.model_classes.ShoppingList",
            "fields": [{"type": {"type": "array", "items": "string", "logicalType": "set"}, "name": "foo"}],
        }
        data = {"foo": ["one", "two"]}
        serialized = MinosAvroProtocol.encode(data, schema)
        deserialized = MinosAvroProtocol.decode(serialized)
        self.assertEqual(data, deserialized)

    def test_union_schema(self):
        serialized = MinosAvroProtocol.encode("one", [["string", "int"]])

        schema = MinosAvroProtocol.decode_schema(serialized)
        self.assertEqual(["string", "int"], schema)

        data = MinosAvroProtocol.decode(serialized)
        self.assertEqual("one", data)

    def test_batch_mode(self):
        serialized = MinosAvroProtocol.encode(["one", 1], [["string", "int"]], batch_mode=True)

        schema = MinosAvroProtocol.decode_schema(serialized)
        self.assertEqual(["string", "int"], schema)

        data = MinosAvroProtocol.decode(serialized, batch_mode=True)
        self.assertEqual(["one", 1], data)

    def test_batch_mode_raises(self):
        serialized = MinosAvroProtocol.encode(["one", 1], [["string", "int"]], batch_mode=True)

        schema = MinosAvroProtocol.decode_schema(serialized)
        self.assertEqual(["string", "int"], schema)

        with self.assertRaises(MinosProtocolException):
            MinosAvroProtocol.decode(serialized)


if __name__ == "__main__":
    unittest.main()
