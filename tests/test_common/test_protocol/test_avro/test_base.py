"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import unittest

from minos.common import (
    MinosAvroProtocol,
)


class TestMinosAvroProtocol(unittest.TestCase):
    def test_multi_schema(self):
        data = {"cost": float("inf"), "user": {"id": 1234, "username": None}}
        schema = [
            {
                "fields": [{"name": "id", "type": "int"}, {"name": "username", "type": ["string", "null"]}],
                "name": "User",
                "namespace": "tests.model_classes",
                "type": "record",
            },
            {
                "fields": [{"name": "user", "type": ["User", "null"]}, {"name": "cost", "type": "float"}],
                "name": "ShoppingList",
                "namespace": "tests.model_classes",
                "type": "record",
            },
        ]
        serialized = MinosAvroProtocol.encode(data, schema)
        self.assertEqual(True, isinstance(serialized, bytes))

        # decode
        deserialized = MinosAvroProtocol.decode(serialized)
        self.assertEqual(data, deserialized)


if __name__ == "__main__":
    unittest.main()
