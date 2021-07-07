"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""

import unittest

from minos.common import (
    AvroSchemaDecoder,
    ModelType,
)


class TestAvroSchemaDecoder(unittest.TestCase):
    def test_model_type(self):
        expected = ModelType.build("User", {"username": str}, "path.to")
        field_schema = {
            "fields": [{"name": "username", "type": "string"}],
            "name": "User",
            "namespace": "path.to.class",
            "type": "record",
        }

        observed = AvroSchemaDecoder(field_schema).build()
        self.assertEqual(expected, observed)

    def test_model_type_single_module(self):
        expected = ModelType.build("User", {"username": str}, "example")
        field_schema = {
            "fields": [{"name": "username", "type": "string"}],
            "name": "User",
            "namespace": "example",
            "type": "record",
        }

        observed = AvroSchemaDecoder(field_schema).build()
        self.assertEqual(expected, observed)


if __name__ == "__main__":
    unittest.main()
