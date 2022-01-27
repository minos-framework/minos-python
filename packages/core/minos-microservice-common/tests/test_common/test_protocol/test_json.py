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
