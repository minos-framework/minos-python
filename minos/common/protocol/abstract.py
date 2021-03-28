import abc
import typing as t


class MinosBinaryProtocol(abc.ABC):

    @classmethod
    @abc.abstractmethod
    def encode(cls, headers: t.Dict, body: t.Any) -> bytes:
        raise NotImplementedError

    @classmethod
    @abc.abstractmethod
    def decode(cls, data: bytes):
        raise NotImplementedError
