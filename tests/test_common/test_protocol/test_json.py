"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""

import unittest

from minos.common import (
    MinosJsonBinaryProtocol,
)


class TestMinosJsonBinaryProtocol(unittest.TestCase):
    def test_encode_decode(self):
        data = {"foo": "bar", "one": 2, "arr": [{"a": "b"}]}
        encoded = MinosJsonBinaryProtocol.encode(data)
        self.assertIsInstance(encoded, bytes)

        decoded = MinosJsonBinaryProtocol.decode(encoded)
        self.assertEqual(data, decoded)


if __name__ == "__main__":
    unittest.main()
