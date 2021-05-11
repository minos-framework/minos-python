"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import json
from base64 import (
    b64decode,
    b64encode,
)
from typing import (
    Any,
)

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
        return b64encode(json.dumps(data).encode())

    @classmethod
    def decode(cls, data: bytes) -> Any:
        """Decodes the given bytes data.

        :param data: bytes data to be decoded.
        :return: De decoded data.
        """
        return json.loads(b64decode(data.decode()))
