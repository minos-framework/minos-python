import abc
import typing as t
import random

from minos.common.exceptions import MinosMessageException
from minos.common.logs import log
from minos.common.protocol.abstract import MinosBinaryProtocol
from minos.common.protocol.avro import MinosAvroProtocol


class MinosBaseRequest(abc.ABC):

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
        if self._body:
            log.debug("imported with body")
            return self._encoder(self._headers, self._body)
        return self._encoder(self._headers)

    def addHeader(self, key: str, value: t.Any):
        self._headers[key] = value
        return self

    def addBody(self, body_val: t.Any):
        if isinstance(body_val, dict):
            self._body = {}
        elif isinstance(body_val, list):
            self._body = []
        self._body = body_val
        return self

    def addMethod(self, method: str):
        self._headers['method'] = method
        return self

    def addPath(self, path: str):
        self._headers['path'] = path
        return self

    def sub_build(self):
        ...

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
        self._headers['id'] = random.randint(0, 1000)

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
        self._headers = decoded_dict['headers']
        if "body" in decoded_dict:
            self._body = decoded_dict['body']


class MinosRPCHeadersRequest(MinosBaseRequest):
    """
    Minos RPC Headers Request Class

    In the Minos RPC protocol the Headers are sent in a separete call.
    The Headers get the ID and the Action
    in the future more parameters are take in count
    """

    @property
    def action(self):
        return self._headers['action']

    def addMethod(self, method: str):
        """
        the RPC does not support the classic method call
        """
        raise AttributeError("this method not exist")

    def addPath(self, path: str):
        """
        the RPC does not support the path
        """
        raise AttributeError("this method not exist")

    def addAction(self, action: str):
        """
        RPC support the action header parameter
        the action correspond to the RPC command
        """
        self._headers['action'] = action
        return self

    def sub_build(self):
        """
        instance more variables for this specific Message class
        """
        self._headers['type'] = "headers"


class MinosRPCBodyRequest(MinosBaseRequest):
    """
    Minos RPC Body Class

    The body are sent after the Headers.
    The Body have as Headers an ID, that correspond of the ID of the Headers call.
    and a set of atttributes that must be passed to the command function.
    """

    def addId(self, id: int):
        self._headers['id'] = id
        return self

    def addMethod(self, method: str):
        raise NotImplementedError

    def addPath(self, path: str):
        raise NotImplementedError

    def sub_build(self):
        """
        instance more variables for this specific Message class
        """
        self._headers['type'] = "body"


class MinosRequest(object):

    @staticmethod
    def build(request_clas: MinosBaseRequest, binary: MinosBinaryProtocol = MinosAvroProtocol):
        """
        this is the Builder that create specific Requests Objects
        """
        request_instance = request_clas()
        if isinstance(request_instance, MinosBaseRequest):
            request_instance.binary_class = binary
            request_instance.build()
            return request_instance

        else:
            raise MinosMessageException("The request class must extend MinosBaseRequest")

    @staticmethod
    def load(data: bytes, request_class: MinosBaseRequest, binary: MinosBinaryProtocol = MinosAvroProtocol):
        """
        load binary data and convert in the Message Request Instance given
        """
        request_instance = request_class()
        if isinstance(request_instance, MinosBaseRequest):
            request_instance.binary_class = binary
            request_instance.load(data)
        return request_instance
