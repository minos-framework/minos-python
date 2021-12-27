from __future__ import (
    annotations,
)

from collections.abc import (
    Awaitable,
    Callable,
)
from inspect import (
    isawaitable,
)
from typing import (
    Any,
    Optional,
    Union,
)
from uuid import (
    UUID,
)

from .abc import (
    Request,
)

sentinel = object()
ContentAction = Callable[[Any, ...], Union[Any, Awaitable[Any]]]
ParamsAction = Callable[[dict[str, Any], ...], Union[dict[str, Any], Awaitable[dict[str, Any]]]]


class WrappedRequest(Request):
    """Wrapped Request class."""

    def __init__(
        self,
        base: Request,
        content_action: Optional[ContentAction] = None,
        params_action: Optional[ParamsAction] = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.base = base
        self.content_action = content_action
        self.params_action = params_action
        self._computed_content = sentinel
        self._computed_params = sentinel

    @property
    def user(self) -> Optional[UUID]:
        """
        Returns the UUID of the user making the Request.
        """
        return self.base.user

    @property
    def has_content(self) -> bool:
        """Check if the request has params.

        :return: ``True`` if it has params or ``False`` otherwise.
        """
        return self.base.has_content

    async def _content(self, **kwargs) -> Any:
        if self.content_action is None:
            return await self.base.content()

        if self._computed_content is sentinel:
            content = self.content_action(await self.base.content(), **kwargs)
            if isawaitable(content):
                content = await content
            self._computed_content = content
        return self._computed_content

    @property
    def has_params(self) -> bool:
        """Check if the request has params.

        :return: ``True`` if it has params or ``False`` otherwise.
        """
        return self.base.has_params

    async def _params(self, **kwargs) -> Any:
        if self.params_action is None:
            return await self.base.params()

        if self._computed_params is sentinel:
            params = self.params_action(await self.base.params(), **kwargs)
            if isawaitable(params):
                params = await params
            self._computed_params = params
        return self._computed_params

    def __eq__(self, other: WrappedRequest) -> bool:
        return (
            isinstance(other, type(self))
            and self.base == other.base
            and self.content_action == other.content_action
            and self.params_action == other.params_action
        )

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.base!r}, {self.content_action!r}, {self.params_action!r})"
