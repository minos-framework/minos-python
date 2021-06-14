"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""

import unittest

from minos.common import (
    AvroSchemaDecoder,
)


class TestAvroSchemaDecoder(unittest.TestCase):
    def test_typed_dict(self):
        field_schema = {
            "fields": [{"name": "username", "type": "string"}],
            "name": "User",
            "namespace": "path.to.class",
            "type": "record",
        }

        observed = AvroSchemaDecoder(field_schema).build()

        self.assertEqual("path.to.User", observed.__name__)
        self.assertEqual({"username": str}, observed.__annotations__)

    def test_typed_dict_single_module(self):
        field_schema = {
            "fields": [{"name": "username", "type": "string"}],
            "name": "User",
            "namespace": "example",
            "type": "record",
        }

        observed = AvroSchemaDecoder(field_schema).build()

        self.assertEqual("example.User", observed.__name__)
        self.assertEqual({"username": str}, observed.__annotations__)


if __name__ == "__main__":
    unittest.main()
