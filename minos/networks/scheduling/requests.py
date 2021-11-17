from __future__ import (
    annotations,
)

from datetime import (
    datetime,
)
from typing import (
    Optional,
)
from uuid import (
    UUID,
)

from minos.common import (
    DeclarativeModel,
)

from ..requests import (
    Request,
    ResponseException,
)


class ScheduledRequest(Request):
    """Scheduling Request class."""

    def __init__(self, scheduled_at: datetime):
        super().__init__()
        self._scheduled_at = scheduled_at

    @property
    def user(self) -> Optional[UUID]:
        """The user of the request.

        :return: Always return ``None`` as scheduled request are launched by the system.
        """
        return None

    async def content(self, **kwargs) -> ScheduledRequestContent:
        """Get the request content.

        :param kwargs: Additional named arguments.
        :return: A ``ScheduledRequestContent` intance.`.
        """
        return self._content

    def __eq__(self, other: Request) -> bool:
        return isinstance(other, type(self)) and self._content == other._content

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self._content!r})"

    @property
    def _content(self) -> ScheduledRequestContent:
        return ScheduledRequestContent(self._scheduled_at)


class ScheduledRequestContent(DeclarativeModel):
    """Scheduling Request Content class."""

    scheduled_at: datetime


class ScheduledResponseException(ResponseException):
    """Scheduled Response Exception class."""
