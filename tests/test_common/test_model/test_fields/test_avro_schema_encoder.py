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
    AvroSchemaEncoder,
)


class TestAvroSchemaDecoder(unittest.TestCase):
    def test_typed_dict(self):
        expected = {
            "name": "class",
            "type": {
                "fields": [{"name": "username", "type": "string"}],
                "name": "User",
                "namespace": "path.to.class",
                "type": "record",
            },
        }

        observed = AvroSchemaEncoder("class", TypedDict("path.to.User", {"username": str})).build()
        self.assertEqual(expected, observed)


if __name__ == "__main__":
    unittest.main()
