import pytest

from minos.common.protocol.avro import MinosAvroProtocol, MinosAvroValuesDatabase


def test_encoder_decoder_with_body_dict():
    headers = {'id': 123, 'action': 'get'}
    body = {'message': "this is a message"}
    data_return_bytes = MinosAvroProtocol.encode(headers, body)
    assert isinstance(data_return_bytes, bytes) == True

    # decode
    returned_dict = MinosAvroProtocol.decode(data_return_bytes)
    assert returned_dict['headers']['action'] == 'get'
    assert returned_dict['headers']['id'] == 123
    assert returned_dict['body']['message'] == "this is a message"


def test_encoder_decoder_with_body_string():
    headers = {'id': 123, 'action': 'get'}
    body = "this is a message"
    data_return_bytes = MinosAvroProtocol.encode(headers, body)
    assert isinstance(data_return_bytes, bytes) == True

    # decode
    returned_dict = MinosAvroProtocol.decode(data_return_bytes)
    assert returned_dict['headers']['action'] == 'get'
    assert returned_dict['headers']['id'] == 123
    assert returned_dict['body'] == "this is a message"


def test_encoder_decoder_with_body_int():
    headers = {'id': 123, 'action': 'get'}
    body = 222
    data_return_bytes = MinosAvroProtocol.encode(headers, body)
    assert isinstance(data_return_bytes, bytes) == True

    # decode
    returned_dict = MinosAvroProtocol.decode(data_return_bytes)
    assert returned_dict['headers']['action'] == 'get'
    assert returned_dict['headers']['id'] == 123
    assert returned_dict['body'] == 222


def test_encoder_decoder_with_body_array():
    headers = {'id': 123, 'action': 'get'}
    body = [
        "testing",
        "testing2"
    ]
    data_return_bytes = MinosAvroProtocol.encode(headers, body)
    assert isinstance(data_return_bytes, bytes) == True

    # decode
    returned_dict = MinosAvroProtocol.decode(data_return_bytes)
    assert returned_dict['headers']['action'] == 'get'
    assert returned_dict['headers']['id'] == 123
    assert returned_dict['body'][0] == "testing"
    assert returned_dict['body'][1] == "testing2"


def test_encoder_decoder_without_body():
    headers = {'id': 123, 'action': 'get'}
    data_return_bytes = MinosAvroProtocol.encode(headers)
    assert isinstance(data_return_bytes, bytes) == True

    # decode
    returned_dict = MinosAvroProtocol.decode(data_return_bytes)
    assert returned_dict['headers']['action'] == 'get'
    assert returned_dict['headers']['id'] == 123


def test_encoder_decoder_database_values_string():
    value = "String Test"
    data_return_bytes = MinosAvroValuesDatabase.encode(value)
    assert isinstance(data_return_bytes, bytes) == True

    # decode
    returned_string = MinosAvroValuesDatabase.decode(data_return_bytes)
    assert returned_string == "String Test"


def test_encoder_decoder_database_values_dictionary():
    value = {"first": "this is a string", "second": 123}
    data_return_bytes = MinosAvroValuesDatabase.encode(value)
    assert isinstance(data_return_bytes, bytes) == True

    # decode
    returned_string = MinosAvroValuesDatabase.decode(data_return_bytes)
    assert returned_string["first"] == "this is a string"
    assert returned_string["second"] == 123


def test_encoder_decoder_database_values_list():
    value = ["first", "second"]
    data_return_bytes = MinosAvroValuesDatabase.encode(value)
    assert isinstance(data_return_bytes, bytes) == True

    # decode
    returned_string = MinosAvroValuesDatabase.decode(data_return_bytes)
    assert returned_string[0] == "first"
    assert returned_string[1] == "second"
