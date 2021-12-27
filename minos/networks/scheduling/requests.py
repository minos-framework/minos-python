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

from cached_property import (
    cached_property,
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

    def __init__(self, scheduled_at: datetime, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._scheduled_at = scheduled_at

    @property
    def user(self) -> Optional[UUID]:
        """The user of the request.

        :return: Always return ``None`` as scheduled request are launched by the system.
        """
        return None

    @property
    def has_content(self) -> bool:
        """Check if the request has content.

        :return: ``True`` if it has content or ``False`` otherwise.
        """
        return True

    async def _content(self, **kwargs) -> ScheduledRequestContent:
        return self._content_value

    @property
    def has_params(self) -> bool:
        """Check if the request has params.

        :return: ``True`` if it has params or ``False`` otherwise.
        """
        return False

    def __eq__(self, other: Request) -> bool:
        return isinstance(other, type(self)) and self._content_value == other._content_value

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self._content_value!r})"

    @cached_property
    def _content_value(self) -> ScheduledRequestContent:
        return ScheduledRequestContent(self._scheduled_at)


class ScheduledRequestContent(DeclarativeModel):
    """Scheduling Request Content class."""

    scheduled_at: datetime


class ScheduledResponseException(ResponseException):
    """Scheduled Response Exception class."""
