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
from inspect import (
    isawaitable,
)
from typing import (
    Any,
    Callable,
    Optional,
)
from uuid import (
    UUID,
)

from minos.common import (
    AvroDataEncoder,
)

from .exceptions import (
    MinosException,
)


class Request(ABC):
    """Request interface."""

    @property
    @abstractmethod
    def user(self) -> Optional[UUID]:
        """
        Returns the UUID of the user making the Request.
        """
        raise NotImplementedError

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


class WrappedRequest(Request):
    """Wrapped Request class."""

    def __init__(self, base: Request, action: Callable[[Any, ...], Any]):
        self.base = base
        self.action = action
        self._content = None

    @property
    def user(self) -> Optional[UUID]:
        """
        Returns the UUID of the user making the Request.
        """
        return self.base.user

    async def content(self, **kwargs) -> Any:
        """Get the request content.

        :param kwargs: Additional named arguments.
        :return: A list of instances.
        """
        if self._content is None:
            content = self.action(await self.base.content(), **kwargs)
            if isawaitable(content):
                content = await content
            self._content = content
        return self._content

    def __eq__(self, other: WrappedRequest) -> bool:
        return type(self) == type(other) and self.base == other.base and self.action == other.action

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.base!r}, {self.action!r})"


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
