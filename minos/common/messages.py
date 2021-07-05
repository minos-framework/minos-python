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
    Model,
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

    __slots__ = "_items"

    def __init__(self, items: Any):
        if not isinstance(items, list):
            items = [items]
        self._items = items

    # noinspection PyUnusedLocal
    async def content(self, **kwargs) -> Any:
        """Response content.

        :param kwargs: Additional named arguments.
        :return: A list of items.
        """
        return self._items

    # noinspection PyUnusedLocal
    async def raw_content(self, **kwargs) -> Any:
        """Raw response content.

        :param kwargs: Additional named arguments.
        :return: A list of raw items.
        """
        return [item if not isinstance(item, Model) else item.avro_data for item in self._items]

    def __eq__(self, other: Response) -> bool:
        return type(self) == type(other) and self._items == other._items

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self._items!r})"


class ResponseException(MinosException):
    """Response Exception class."""
