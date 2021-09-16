import unittest

from minos.common import (
    MinosAvroDatabaseProtocol,
    MinosProtocolException,
)


class TestMinosAvroDatabaseProtocol(unittest.TestCase):
    def test_encoder_decoder_database_values_string(self):
        value = "String Test"
        data_return_bytes = MinosAvroDatabaseProtocol.encode(value)
        self.assertEqual(True, isinstance(data_return_bytes, bytes))

        # decode
        returned_string = MinosAvroDatabaseProtocol.decode(data_return_bytes)
        self.assertEqual("String Test", returned_string)

    def test_encoder_decoder_database_values_dictionary(self):
        value = {"first": "this is a string", "second": 123}
        data_return_bytes = MinosAvroDatabaseProtocol.encode(value)
        self.assertEqual(True, isinstance(data_return_bytes, bytes))

        # decode
        returned_string = MinosAvroDatabaseProtocol.decode(data_return_bytes)
        self.assertEqual("this is a string", returned_string["first"])
        self.assertEqual(123, returned_string["second"])

    def test_encoder_decoder_database_values_list(self):
        value = ["first", "second"]
        data_return_bytes = MinosAvroDatabaseProtocol.encode(value)
        self.assertEqual(True, isinstance(data_return_bytes, bytes))

        # decode
        returned_string = MinosAvroDatabaseProtocol.decode(data_return_bytes)
        self.assertEqual("first", returned_string[0])
        self.assertEqual("second", returned_string[1])

    def test_encode_raises(self):
        with self.assertRaises(MinosProtocolException):
            MinosAvroDatabaseProtocol.encode({1234: 5678})

    def test_decode_raises(self):
        with self.assertRaises(MinosProtocolException):
            MinosAvroDatabaseProtocol.decode(bytes())


if __name__ == "__main__":
    unittest.main()
