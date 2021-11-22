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

    def __init__(self, raw: BrokerMessage):
        self.raw = raw

    def __eq__(self, other: BrokerRequest) -> bool:
        return type(self) == type(other) and self.raw == other.raw

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.raw!r})"

    @property
    def user(self) -> Optional[UUID]:
        """
        Returns the UUID of the user making the Request.
        """
        return getattr(self.raw, "user", None)

    async def content(self, **kwargs) -> Any:
        """Request content.

        :param kwargs: Additional named arguments.
        :return: The content.
        """
        data = self.raw.data
        return data


class BrokerResponse(Response):
    """Handler Response class."""


class BrokerResponseException(ResponseException):
    """Handler Response Exception class."""
