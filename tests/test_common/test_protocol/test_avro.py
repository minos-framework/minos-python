"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import unittest

from minos.common import (
    MinosAvroProtocol,
    MinosAvroValuesDatabase,
    MinosProtocolException,
)


class TestMinosAvroProtocol(unittest.TestCase):
    def test_encoder_decoder_with_body_dict(self):
        headers = {"id": 123, "action": "get"}
        body = {"message": "this is a message"}
        data_return_bytes = MinosAvroProtocol.encode(headers, body)
        self.assertEqual(True, isinstance(data_return_bytes, bytes))

        # decode
        returned_dict = MinosAvroProtocol.decode(data_return_bytes)
        self.assertEqual("get", returned_dict["headers"]["action"])
        self.assertEqual(123, returned_dict["headers"]["id"])
        self.assertEqual("this is a message", returned_dict["body"]["message"])

    def test_encoder_decoder_with_body_string(self):
        headers = {"id": 123, "action": "get"}
        body = "this is a message"
        data_return_bytes = MinosAvroProtocol.encode(headers, body)
        self.assertEqual(True, isinstance(data_return_bytes, bytes))

        # decode
        returned_dict = MinosAvroProtocol.decode(data_return_bytes)
        self.assertEqual("get", returned_dict["headers"]["action"])
        self.assertEqual(123, returned_dict["headers"]["id"])
        self.assertEqual("this is a message", returned_dict["body"])

    def test_encoder_decoder_with_body_int(self):
        headers = {"id": 123, "action": "get"}
        body = 222
        data_return_bytes = MinosAvroProtocol.encode(headers, body)
        self.assertEqual(True, isinstance(data_return_bytes, bytes))

        # decode
        returned_dict = MinosAvroProtocol.decode(data_return_bytes)
        self.assertEqual("get", returned_dict["headers"]["action"])
        self.assertEqual(123, returned_dict["headers"]["id"])
        self.assertEqual(222, returned_dict["body"])

    def test_encoder_decoder_with_body_array(self):
        headers = {"id": 123, "action": "get"}
        body = ["testing", "testing2"]
        data_return_bytes = MinosAvroProtocol.encode(headers, body)
        self.assertEqual(True, isinstance(data_return_bytes, bytes))

        # decode
        returned_dict = MinosAvroProtocol.decode(data_return_bytes)
        self.assertEqual("get", returned_dict["headers"]["action"])
        self.assertEqual(123, returned_dict["headers"]["id"])
        self.assertEqual("testing", returned_dict["body"][0])
        self.assertEqual("testing2", returned_dict["body"][1])

    def test_encoder_decoder_without_body(self):
        headers = {"id": 123, "action": "get"}
        data_return_bytes = MinosAvroProtocol.encode(headers)
        self.assertEqual(True, isinstance(data_return_bytes, bytes))

        # decode
        returned_dict = MinosAvroProtocol.decode(data_return_bytes)
        self.assertEqual("get", returned_dict["headers"]["action"])
        self.assertEqual(123, returned_dict["headers"]["id"])

    def test_encode_raises(self):
        with self.assertRaises(MinosProtocolException):
            MinosAvroProtocol.encode(3)

    def test_decode_raises(self):
        with self.assertRaises(MinosProtocolException):
            MinosAvroProtocol.decode(bytes())


class TestMinosAvroValuesDatabase(unittest.TestCase):
    def test_encoder_decoder_database_values_string(self):
        value = "String Test"
        data_return_bytes = MinosAvroValuesDatabase.encode(value)
        self.assertEqual(True, isinstance(data_return_bytes, bytes))

        # decode
        returned_string = MinosAvroValuesDatabase.decode(data_return_bytes)
        self.assertEqual("String Test", returned_string)

    def test_encoder_decoder_database_values_dictionary(self):
        value = {"first": "this is a string", "second": 123}
        data_return_bytes = MinosAvroValuesDatabase.encode(value)
        self.assertEqual(True, isinstance(data_return_bytes, bytes))

        # decode
        returned_string = MinosAvroValuesDatabase.decode(data_return_bytes)
        self.assertEqual("this is a string", returned_string["first"])
        self.assertEqual(123, returned_string["second"])

    def test_encoder_decoder_database_values_list(self):
        value = ["first", "second"]
        data_return_bytes = MinosAvroValuesDatabase.encode(value)
        self.assertEqual(True, isinstance(data_return_bytes, bytes))

        # decode
        returned_string = MinosAvroValuesDatabase.decode(data_return_bytes)
        self.assertEqual("first", returned_string[0])
        self.assertEqual("second", returned_string[1])

    def test_encode_raises(self):
        with self.assertRaises(MinosProtocolException):
            MinosAvroValuesDatabase.encode({}, schema={"": ""})

    def test_decode_raises(self):
        with self.assertRaises(MinosProtocolException):
            MinosAvroValuesDatabase.decode(bytes())


if __name__ == "__main__":
    unittest.main()
