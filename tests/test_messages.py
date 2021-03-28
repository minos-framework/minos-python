import pytest

from minos.common.messages.request import MinosRequest, MinosRPCHeadersRequest, \
    MinosRPCBodyRequest
from minos.common.messages.response import MinosRPCResponse, MinosResponse
from minos.common.protocol.avro import MinosAvroProtocol


def test_request_avro_request_get_binary_headers():
    class_request: MinosRPCHeadersRequest = MinosRequest.build(MinosRPCHeadersRequest) \
        .addAction("with_args")
    binary_data = class_request.binary
    imported_request_class: MinosRPCHeadersRequest = MinosRequest.load(binary_data, MinosRPCHeadersRequest)

    assert imported_request_class.action == "with_args"


def test_request_avro_build_request_headers():
    class_request: MinosRPCHeadersRequest = MinosRequest.build(MinosRPCHeadersRequest) \
        .addAction("with_args")

    assert class_request.action == "with_args"


def test_request_avro_import_request_headers():
    headers = {'id': 123, 'action': 'get'}
    data_return_bytes = MinosAvroProtocol.encode(headers)

    class_request: MinosRPCHeadersRequest = MinosRequest.load(data_return_bytes, MinosRPCHeadersRequest)

    assert class_request.id == 123


def test_request_avro_build_request_body():
    class_request: MinosRPCBodyRequest = MinosRequest.build(MinosRPCBodyRequest).addId(123)
    assert class_request.id == 123


def test_request_avro_import_request_body():
    headers = {'id': 123, 'action': 'get'}
    body = {"test": "this is a test"}
    data_return_bytes = MinosAvroProtocol.encode(headers, body)

    class_request: MinosRPCBodyRequest = MinosRequest.load(data_return_bytes, MinosRPCBodyRequest)

    assert class_request.body['test'] == "this is a test"


def test_request_avro_attributes_error_request():
    try:
        class_request: MinosRPCHeadersRequest = MinosRequest.build(MinosRPCHeadersRequest).addMethod("GET")
        assert False == True
    except AttributeError as e:
        assert True == True


def test_request_avro_import_response():
    headers = {'id': 123}
    body = {"test": "this is a response test"}
    data_return_bytes = MinosAvroProtocol.encode(headers, body)

    class_response: MinosRPCResponse = MinosResponse.load(data_return_bytes, MinosRPCResponse)

    assert class_response.body['test'] == "this is a response test"


def test_request_avro_build_response():
    class_response: MinosRPCResponse = MinosResponse.build(MinosRPCResponse).addId(123).addBody("Test Response Body")

    assert class_response.body == "Test Response Body"
    assert class_response.id == 123


def test_request_avro_error_response_id():
    try:
        class_response: MinosRPCResponse = MinosResponse.build(MinosRPCResponse).addBody("Test Response Body")
        class_response.binary
        assert False == True
    except AttributeError:
        assert True == True


def test_request_avro_get_binary_response():
    try:
        class_response: MinosRPCResponse = MinosResponse.build(MinosRPCResponse).addId(123).addBody(
            "Test Response Body")
        binary_format = class_response.binary
        imported_response_class: MinosRPCResponse = MinosResponse.load(binary_format, MinosRPCResponse)
        assert imported_response_class.id == 123
        assert imported_response_class.body == "Test Response Body"
    except AttributeError:
        assert False == True
