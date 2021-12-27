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

    def __init__(self, raw: BrokerMessage, *args, **kwargs):
        super().__init__(*args, **kwargs)
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

    @property
    def has_content(self) -> bool:
        """Check if the request has content.

        :return: ``True`` if it has content or ``False`` otherwise.
        """
        return True

    async def _content(self, **kwargs) -> Any:
        return self.raw.data

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
