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
)

from minos.common import (
    DeclarativeModel,
)

REPLY_TOPIC_CONTEXT_VAR: Final[ContextVar[Optional[str]]] = ContextVar("reply_topic", default=None)


class BrokerMessage(DeclarativeModel):
    """Broker Message class."""

    topic: str
    data: Any
    service_name: str
    saga: Optional[UUID]
    reply_topic: Optional[str]
    user: Optional[UUID]
    status: BrokerMessageStatus
    strategy: BrokerMessageStrategy

    def __init__(
        self,
        topic: str,
        data: Any,
        service_name: str,
        *,
        status: Optional[BrokerMessageStatus] = None,
        strategy: Optional[BrokerMessageStrategy] = None,
        **kwargs
    ):
        if status is None:
            status = BrokerMessageStatus.SUCCESS
        if strategy is None:
            strategy = BrokerMessageStrategy.UNICAST
        super().__init__(topic, data, service_name, status=status, strategy=strategy, **kwargs)

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
