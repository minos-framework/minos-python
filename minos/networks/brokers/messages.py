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
    """Broker Message class."""

    topic: str
    data: Any
    identifier: Optional[UUID]
    reply_topic: Optional[str]
    user: Optional[UUID]
    status: Optional[BrokerMessageStatus]
    service_name: Optional[str]

    def __init__(
        self,
        topic: str,
        data: Any,
        *,
        identifier: Optional[UUID] = None,
        reply_topic: Optional[str] = None,
        user: Optional[UUID] = None,
        status: Optional[BrokerMessageStatus] = None,
        service_name: Optional[str] = None,
        **kwargs
    ):
        super().__init__(topic, data, identifier, reply_topic, user, status, service_name, **kwargs)

    @property
    def ok(self) -> bool:
        """TODO"""
        return self.status == BrokerMessageStatus.SUCCESS


class BrokerMessageStatus(IntEnum):
    """Broker status class."""

    SUCCESS = 200
    ERROR = 400
    SYSTEM_ERROR = 500
