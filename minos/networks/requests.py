from __future__ import (
    annotations,
)

from abc import (
    ABC,
    abstractmethod,
)
from contextvars import (
    ContextVar,
)
from inspect import (
    isawaitable,
)
from typing import (
    Any,
    Callable,
    Final,
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

REQUEST_USER_CONTEXT_VAR: Final[ContextVar[Optional[UUID]]] = ContextVar("user", default=None)


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
        :return: The request content.
        """
        raise NotImplementedError

    @property
    @abstractmethod
    def has_content(self) -> bool:
        """Check if the request has content.

        :return: ``True`` if it has content or ``False`` otherwise.
        """
        raise NotImplementedError

    @abstractmethod
    async def params(self, **kwargs) -> Optional[dict[str, Any]]:
        """Get the request params.

        :param kwargs: Additional named arguments.
        :return: The request params.
        """
        raise NotImplementedError

    @property
    @abstractmethod
    def has_params(self) -> bool:
        """Check if the request has params.

        :return: ``True`` if it has params or ``False`` otherwise.
        """
        raise NotImplementedError

    @abstractmethod
    def __eq__(self, other: Request) -> bool:
        raise NotImplementedError

    @abstractmethod
    def __repr__(self) -> str:
        raise NotImplementedError


sentinel = object()


class WrappedRequest(Request):
    """Wrapped Request class."""

    def __init__(
        self,
        base: Request,
        content_action: Callable[[Any, ...], Any] = None,
        params_action: Callable[[Any, ...], Any] = None,
    ):
        self.base = base
        self.content_action = content_action
        self.params_action = params_action
        self._content = sentinel
        self._params = sentinel

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
        if self.content_action is None:
            return await self.base.content()

        if self._content is sentinel:
            content = self.content_action(await self.base.content(), **kwargs)
            if isawaitable(content):
                content = await content
            self._content = content
        return self._content

    @property
    def has_content(self) -> bool:
        """Check if the request has params.

        :return: ``True`` if it has params or ``False`` otherwise.
        """
        return self.base.has_content

    async def params(self, **kwargs) -> Any:
        """Get the request params.

        :param kwargs: Additional named arguments.
        :return: The request params.
        """

        if self.params_action is None:
            return await self.base.params()

        if self._params is sentinel:
            params = self.params_action(await self.base.params(), **kwargs)
            if isawaitable(params):
                params = await params
            self._params = params
        return self._params

    @property
    def has_params(self) -> bool:
        """Check if the request has params.

        :return: ``True`` if it has params or ``False`` otherwise.
        """
        return self.base.has_params

    def __eq__(self, other: WrappedRequest) -> bool:
        return (
            isinstance(other, type(self))
            and self.base == other.base
            and self.content_action == other.content_action
            and self.params_action == other.params_action
        )

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.base!r}, {self.content_action!r}, {self.params_action!r})"


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

    def __hash__(self):
        return hash(self._data)


class ResponseException(MinosException):
    """Response Exception class."""
