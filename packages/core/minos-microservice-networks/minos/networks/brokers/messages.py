from __future__ import (
    annotations,
)

from contextvars import (
    ContextVar,
)
from enum import (
    Enum,
    IntEnum,
)
from typing import (
    Any,
    Final,
    Optional,
)
from uuid import (
    UUID,
    uuid4,
)

from minos.common import (
    DeclarativeModel,
)

REQUEST_REPLY_TOPIC_CONTEXT_VAR: Final[ContextVar[Optional[str]]] = ContextVar("reply_topic", default=None)
REQUEST_HEADERS_CONTEXT_VAR: Final[ContextVar[Optional[dict[str, str]]]] = ContextVar("headers", default=None)


class BrokerMessage(DeclarativeModel):
    """Broker Message class."""

    topic: str
    data: Any
    identifier: UUID
    reply_topic: Optional[str]
    user: Optional[UUID]
    status: BrokerMessageStatus
    strategy: BrokerMessageStrategy
    headers: dict[str, str]

    def __init__(
        self,
        topic: str,
        data: Any,
        *,
        identifier: Optional[UUID] = None,
        status: Optional[BrokerMessageStatus] = None,
        strategy: Optional[BrokerMessageStrategy] = None,
        headers: Optional[dict[str, str]] = None,
        **kwargs
    ):
        if identifier is None:
            identifier = uuid4()
        if status is None:
            status = BrokerMessageStatus.SUCCESS
        if strategy is None:
            strategy = BrokerMessageStrategy.UNICAST
        if headers is None:
            headers = dict()
        super().__init__(
            topic=topic, data=data, identifier=identifier, status=status, strategy=strategy, headers=headers, **kwargs
        )

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


class BrokerMessageStrategy(str, Enum):
    """Broker Message Strategy class"""

    UNICAST = "unicast"
    MULTICAST = "multicast"
