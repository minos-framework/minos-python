import warnings
from typing import (
    Any,
    Optional,
)
from uuid import (
    UUID,
)

from .abc import (
    REQUEST_USER_CONTEXT_VAR,
    Request,
)

sentinel = object()


class InMemoryRequest(Request):
    """In Memory Request class."""

    def __init__(
        self, content: Any = sentinel, params: dict[str, Any] = sentinel, user: Optional[UUID] = None, *args, **kwargs
    ):
        super().__init__(*args, **kwargs)

        if (context_user := REQUEST_USER_CONTEXT_VAR.get()) is not None:
            if user is not None:
                warnings.warn("The `user` argument will be ignored in favor of the `user` ContextVar", RuntimeWarning)
            user = context_user

        self._content_value = content
        self._params_value = params
        self._user = user

    @property
    def user(self) -> Optional[UUID]:
        """For testing purposes"""
        return self._user

    @property
    def has_content(self) -> bool:
        """Check if the request has content.

        :return: ``True`` if it has content or ``False`` otherwise.
        """
        return self._content_value is not sentinel

    async def _content(self, **kwargs) -> Any:
        return self._content_value

    @property
    def has_params(self) -> bool:
        """Check if the request has params.

        :return: ``True`` if it has params or ``False`` otherwise.
        """
        return self._params_value is not sentinel

    async def _params(self, **kwargs) -> dict[str, Any]:
        return self._params_value

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, type(self))
            and self._content_value == other._content_value
            and self._params_value == other._params_value
            and self._user == other._user
        )

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self._content_value!r}, {self._params_value!r}, {self._user!r})"
