from __future__ import (
    annotations,
)

from enum import (
    IntEnum,
)
from typing import (
    Any,
    Optional,
)
from uuid import (
    UUID,
)

from minos.common import (
    DeclarativeModel,
)


class PublishRequest(DeclarativeModel):
    """Publish request class."""

    topic: str
    data: Any
    identifier: Optional[UUID]
    reply_topic: Optional[str]
    user: Optional[UUID]

    def __init__(
        self,
        topic: str,
        data: Any,
        identifier: Optional[UUID] = None,
        reply_topic: Optional[str] = None,
        user: Optional[UUID] = None,
        *args,
        **kwargs
    ):
        super().__init__(topic, data, identifier, reply_topic, user, *args, **kwargs)


class PublishResponse(DeclarativeModel):
    """Publish response class."""

    topic: str
    data: Any
    identifier: Optional[UUID]
    status: PublishResponseStatus
    service_name: str

    @property
    def ok(self) -> bool:
        """TODO"""
        return self.status == PublishResponseStatus.SUCCESS


class PublishResponseStatus(IntEnum):
    """Publish status class."""

    SUCCESS = 200
    ERROR = 400
    SYSTEM_ERROR = 500
