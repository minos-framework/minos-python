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


class BrokerMessage(DeclarativeModel):
    """TODO"""

    topic: str
    data: Any
    saga: Optional[UUID]
    reply_topic: Optional[str]
    user: Optional[UUID]
    status: Optional[BrokerMessageStatus]
    service_name: Optional[str]

    def __init__(self, topic: str, data: Any, **kwargs):
        super().__init__(topic, data, **kwargs)

    @property
    def ok(self) -> bool:
        """Check if the reply is okay or not.

        :return: ``True`` if the reply is okay or ``False`` otherwise.
        """
        return self.status == BrokerMessageStatus.SUCCESS


class BrokerMessageStatus(IntEnum):
    """Broker Message Status class."""

    SUCCESS = 200
    ERROR = 400
    SYSTEM_ERROR = 500
