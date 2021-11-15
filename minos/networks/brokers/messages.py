from __future__ import (
    annotations,
)

from contextvars import (
    ContextVar,
)
from enum import (
    IntEnum,
)
from typing import (
    Any,
    Final,
    Optional,
)
from uuid import (
    UUID,
)

from minos.common import (
    DeclarativeModel,
)

REPLY_TOPIC_CONTEXT_VAR: Final[ContextVar[Optional[str]]] = ContextVar("reply_topic", default=None)


class Command(DeclarativeModel):
    """Base Command class."""

    topic: str
    data: Any
    saga: Optional[UUID]
    reply_topic: str
    user: Optional[UUID]


class CommandReply(DeclarativeModel):
    """Base Command class."""

    topic: str
    data: Any
    saga: Optional[UUID]
    status: CommandStatus
    service_name: Optional[str]

    @property
    def ok(self) -> bool:
        """Check if the reply is okay or not.

        :return: ``True`` if the reply is okay or ``False`` otherwise.
        """
        return self.status == CommandStatus.SUCCESS


class CommandStatus(IntEnum):
    """Command Status class."""

    SUCCESS = 200
    ERROR = 400
    SYSTEM_ERROR = 500


class Event(DeclarativeModel):
    """Base Event class."""

    topic: str
    data: Any
