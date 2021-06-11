"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import unittest

from minos.common import (
    DynamicModel,
)
from tests.model_classes import (
    Foo,
)


class TestDynamicModel(unittest.TestCase):
    def test_from_avro_bytes(self):
        original = Foo("hello")
        model = DynamicModel.from_avro_bytes(original.avro_bytes)
        self.assertEqual("hello", model.text)

    def test_from_avro_bytes_multiple(self):
        encoded = Foo.to_avro_bytes([Foo("hello"), Foo("bye")])
        decoded = DynamicModel.from_avro_bytes(encoded)
        self.assertEqual("hello", decoded[0].text)
        self.assertEqual("bye", decoded[1].text)

    def test_avro_schema(self):
        expected = [
            {
                "fields": [{"name": "text", "type": "string"}],
                "name": "DynamicModel",
                "namespace": "minos.common.model.dynamic.abc",
                "type": "record",
            }
        ]

        original = Foo("hello")
        model = DynamicModel.from_avro_bytes(original.avro_bytes)
        self.assertEqual(expected, model.avro_schema)


if __name__ == "__main__":
    unittest.main()
