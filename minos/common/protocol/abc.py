"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""

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
    def decode(cls, data: bytes) -> Any:
        """Decodes the given bytes data.

        :param data: bytes data to be decoded.
        :return: De decoded data.
        """
        raise NotImplementedError
