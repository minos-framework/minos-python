from __future__ import (
    annotations,
)

from typing import (
    Any,
    Optional,
)
from uuid import (
    UUID,
)

from cached_property import (
    cached_property,
)

from ...requests import (
    Request,
    Response,
    ResponseException,
)
from ..messages import (
    BrokerMessage,
)


class BrokerRequest(Request):
    """Handler Request class."""

    __slots__ = "raw"

    def __init__(self, raw: BrokerMessage, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.raw = raw

    def __eq__(self, other: BrokerRequest) -> bool:
        return type(self) == type(other) and self.raw == other.raw

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.raw!r})"

    @cached_property
    def user(self) -> Optional[UUID]:
        """
        Returns the UUID of the user making the Request.
        """
        if "user" not in self.headers:
            return None
        return UUID(self.headers["user"])

    @property
    def headers(self) -> dict[str, str]:
        """Get the headers of the request.

        :return: A dictionary in which keys are ``str`` instances and values are ``str`` instances.
        """
        return self.raw.headers

    @property
    def has_content(self) -> bool:
        """Check if the request has content.

        :return: ``True`` if it has content or ``False`` otherwise.
        """
        return True

    async def _content(self, **kwargs) -> Any:
        return self.raw.content

    @property
    def has_params(self) -> bool:
        """Check if the request has params.

        :return: ``True`` if it has params or ``False`` otherwise.
        """
        return False


class BrokerResponse(Response):
    """Handler Response class."""


class BrokerResponseException(ResponseException):
    """Handler Response Exception class."""
