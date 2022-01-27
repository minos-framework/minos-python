import abc
from typing import (
    Any,
)


class MinosBinaryProtocol(abc.ABC):
    """Minos binary encoder / decoder interface."""

    @classmethod
    @abc.abstractmethod
    def encode(cls, *args, **kwargs) -> bytes:
        """Encodes the given value into bytes.

        :param args: Additional positional arguments.
        :param kwargs: Additional named arguments.
        :return: A bytes instance.
        """
        raise NotImplementedError

    @classmethod
    @abc.abstractmethod
    def decode(cls, data: bytes, *args, **kwargs) -> Any:
        """Decodes the given bytes data.

        :param data: bytes data to be decoded.
        :return: De decoded data.
        """
        raise NotImplementedError
