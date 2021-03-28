import abc
import typing as t

from minos.common.exceptions import MinosMessageException
from minos.common.protocol.abstract import MinosBinaryProtocol
from minos.common.protocol.avro import MinosAvroProtocol


class MinosBaseResponse(abc.ABC):
    __slots__ = 'binary_class', '_encoder', '_decoder', '_headers', '_body'

    @property
    def id(self):
        return self._headers['id']

    @property
    def body(self):
        return self._body

    @property
    def headers(self):
        return self._headers

    @property
    def binary(self):
        if "id" not in self._headers:
            raise AttributeError("ID is mandatory, please use addId method before request binary data")

        if self._body:
            return self._encoder(self._headers, self._body)
        return self._encoder(self._headers)

    def addBody(self, body_val: t.Any):
        if isinstance(body_val, dict):
            self._body = {}
        elif isinstance(body_val, list):
            self._body = []
        self._body = body_val
        return self

    def addId(self, id: int):
        self._headers['id'] = id
        return self

    def sub_build(self):
        ...

    def build(self):
        """
        build the instance with all the default informations
        """
        self._prepare_binary_encoder_decoder()
        self._prepare_default_attributes()
        self.sub_build()

    def load(self, data: bytes):
        """
        import the bytes code and obtain the Request
        """
        self._prepare_binary_encoder_decoder()
        decoded_dict = self._decoder(data)
        self._headers = {}
        if "id" not in decoded_dict['headers']:
            raise AttributeError("The Response dosn't have any ID configured, not valid!")

        self._headers = decoded_dict['headers']
        if "body" in decoded_dict:
            self._body = decoded_dict['body']

    def _prepare_binary_encoder_decoder(self):
        """
        configure the Encoder and the Decoder methods from the Binary Class Instance passed from the Builder
        """
        self._encoder = self.binary_class.encode
        self._decoder = self.binary_class.decode

    def _prepare_default_attributes(self):
        """
        instanciate the default class attributes like headers dict, body as None and the headers id
        """
        self._headers = {}
        self._body = None


class MinosRPCResponse(MinosBaseResponse):
    ...


class MinosResponse(object):

    @staticmethod
    def build(response_class: MinosBaseResponse, binary: MinosBinaryProtocol = MinosAvroProtocol):
        request_instance = response_class()
        if isinstance(request_instance, MinosBaseResponse):
            request_instance.binary_class = binary
            request_instance.build()
            return request_instance

        else:
            raise MinosMessageException("The request class must extend MinosBaseResponse")

    @staticmethod
    def load(data: bytes, response_class: MinosBaseResponse, binary: MinosBinaryProtocol = MinosAvroProtocol):
        """
        load binary data and convert in the Message Request Instance given
        """
        request_instance = response_class()
        if isinstance(request_instance, MinosBaseResponse):
            request_instance.binary_class = binary
            request_instance.load(data)
        return request_instance
