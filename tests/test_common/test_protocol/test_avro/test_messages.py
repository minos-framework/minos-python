import unittest

from minos.common import (
    MinosAvroMessageProtocol,
    MinosProtocolException,
)


class TestMinosAvroMessageProtocol(unittest.TestCase):
    def test_encoder_decoder_with_body_dict(self):
        headers = {"id": 123, "action": "get"}
        body = {"message": "this is a message"}
        data_return_bytes = MinosAvroMessageProtocol.encode(headers, body)
        self.assertEqual(True, isinstance(data_return_bytes, bytes))

        # decode
        returned_dict = MinosAvroMessageProtocol.decode(data_return_bytes)
        self.assertEqual("get", returned_dict["headers"]["action"])
        self.assertEqual(123, returned_dict["headers"]["id"])
        self.assertEqual("this is a message", returned_dict["body"]["message"])

    def test_encoder_decoder_with_body_string(self):
        headers = {"id": 123, "action": "get"}
        body = "this is a message"
        data_return_bytes = MinosAvroMessageProtocol.encode(headers, body)
        self.assertEqual(True, isinstance(data_return_bytes, bytes))

        # decode
        returned_dict = MinosAvroMessageProtocol.decode(data_return_bytes)
        self.assertEqual("get", returned_dict["headers"]["action"])
        self.assertEqual(123, returned_dict["headers"]["id"])
        self.assertEqual("this is a message", returned_dict["body"])

    def test_encoder_decoder_with_body_int(self):
        headers = {"id": 123, "action": "get"}
        body = 222
        data_return_bytes = MinosAvroMessageProtocol.encode(headers, body)
        self.assertEqual(True, isinstance(data_return_bytes, bytes))

        # decode
        returned_dict = MinosAvroMessageProtocol.decode(data_return_bytes)
        self.assertEqual("get", returned_dict["headers"]["action"])
        self.assertEqual(123, returned_dict["headers"]["id"])
        self.assertEqual(222, returned_dict["body"])

    def test_encoder_decoder_with_body_array(self):
        headers = {"id": 123, "action": "get"}
        body = ["testing", "testing2"]
        data_return_bytes = MinosAvroMessageProtocol.encode(headers, body)
        self.assertEqual(True, isinstance(data_return_bytes, bytes))

        # decode
        returned_dict = MinosAvroMessageProtocol.decode(data_return_bytes)
        self.assertEqual("get", returned_dict["headers"]["action"])
        self.assertEqual(123, returned_dict["headers"]["id"])
        self.assertEqual("testing", returned_dict["body"][0])
        self.assertEqual("testing2", returned_dict["body"][1])

    def test_encoder_decoder_without_body(self):
        headers = {"id": 123, "action": "get"}
        data_return_bytes = MinosAvroMessageProtocol.encode(headers)
        self.assertEqual(True, isinstance(data_return_bytes, bytes))

        # decode
        returned_dict = MinosAvroMessageProtocol.decode(data_return_bytes)
        self.assertEqual("get", returned_dict["headers"]["action"])
        self.assertEqual(123, returned_dict["headers"]["id"])

    def test_encode_raises(self):
        with self.assertRaises(MinosProtocolException):
            # noinspection PyTypeChecker
            MinosAvroMessageProtocol.encode(3)

    def test_decode_raises(self):
        with self.assertRaises(MinosProtocolException):
            MinosAvroMessageProtocol.decode(bytes())


if __name__ == "__main__":
    unittest.main()
