"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from __future__ import (
    annotations,
)

from abc import (
    ABC,
    abstractmethod,
)
from typing import (
    Any,
)

from .exceptions import (
    MinosException,
)
from .model import (
    AvroDataEncoder,
)


class Request(ABC):
    """Request interface."""

    @abstractmethod
    async def content(self, **kwargs) -> Any:
        """Get the request content.

        :param kwargs: Additional named arguments.
        :return: A list of instances.
        """
        raise NotImplementedError

    @abstractmethod
    def __eq__(self, other: Request) -> bool:
        raise NotImplementedError

    @abstractmethod
    def __repr__(self) -> str:
        raise NotImplementedError


class Response:
    """Response definition."""

    __slots__ = "_data"

    def __init__(self, data: Any):
        self._data = data

    # noinspection PyUnusedLocal
    async def content(self, **kwargs) -> Any:
        """Response content.

        :param kwargs: Additional named arguments.
        :return: A list of items.
        """
        return self._data

    # noinspection PyUnusedLocal
    async def raw_content(self, **kwargs) -> Any:
        """Raw response content.

        :param kwargs: Additional named arguments.
        :return: A list of raw items.
        """
        return AvroDataEncoder(self._data).build()

    def __eq__(self, other: Response) -> bool:
        return type(self) == type(other) and self._data == other._data

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self._data!r})"


class ResponseException(MinosException):
    """Response Exception class."""
