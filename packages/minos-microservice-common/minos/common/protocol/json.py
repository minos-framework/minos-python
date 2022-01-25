from typing import (
    Any,
)

import orjson

from .abc import (
    MinosBinaryProtocol,
)


class MinosJsonBinaryProtocol(MinosBinaryProtocol):
    """JSON based binary encoder / decoder implementation."""

    @classmethod
    def encode(cls, data: Any, *args, **kwargs) -> bytes:
        """Encodes the given value into bytes.

        :param data: Data to be encoded.
        :param args: Additional positional arguments.
        :param kwargs: Additional named arguments.
        :return: A bytes instance.
        """
        return orjson.dumps(data)

    @classmethod
    def decode(cls, data: bytes, *args, **kwargs) -> Any:
        """Decodes the given bytes data.

        :param data: bytes data to be decoded.
        :return: De decoded data.
        """
        return orjson.loads(data)
