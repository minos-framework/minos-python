from __future__ import (
    annotations,
)

import warnings
from contextvars import (
    ContextVar,
)
from enum import (
    Enum,
    IntEnum,
)
from functools import (
    total_ordering,
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


@total_ordering
class BrokerMessage(DeclarativeModel):
    """Broker Message class."""

    topic: str
    identifier: UUID
    reply_topic: Optional[str]
    strategy: BrokerMessageStrategy  # FIXME: Remove this attribute!

    content: BrokerMessageContent

    def __init__(
        self,
        topic: str,
        data: Any = None,
        *,
        identifier: Optional[UUID] = None,
        strategy: Optional[BrokerMessageStrategy] = None,
        content: Optional[BrokerMessageContent] = None,
        **kwargs
    ):
        if identifier is None:
            identifier = uuid4()
        if strategy is None:
            strategy = BrokerMessageStrategy.UNICAST
        if content is None:
            content = BrokerMessageContent(topic, data, **kwargs)
        super().__init__(topic=topic, identifier=identifier, strategy=strategy, content=content, **kwargs)

    @property
    def ok(self) -> bool:
        """Check if the reply is okay or not.

        :return: ``True`` if the reply is okay or ``False`` otherwise.
        """
        warnings.warn("The `BrokerMessage.ok` attribute has being deprecated", DeprecationWarning)
        return self.content.ok

    @property
    def status(self) -> BrokerMessageStatus:
        """TODO"""
        warnings.warn("The `BrokerMessage.status` attribute has being deprecated", DeprecationWarning)
        return self.content.status

    @property
    def data(self) -> Any:
        """TODO"""
        warnings.warn("The `BrokerMessage.data` attribute has being deprecated", DeprecationWarning)
        return self.content.data

    def __lt__(self, other: Any) -> bool:
        # noinspection PyBroadException
        return isinstance(other, type(self)) and self.content < other.content


@total_ordering
class BrokerMessageContent(DeclarativeModel):
    """TODO"""

    action: str
    data: Any
    status: BrokerMessageStatus
    headers: dict[str, str]

    def __init__(
        self,
        action: str,
        data: Any,
        headers: Optional[dict[str, str]] = None,
        status: Optional[BrokerMessageStatus] = None,
        **kwargs
    ):
        if headers is None:
            headers = dict()
        if status is None:
            status = BrokerMessageStatus.SUCCESS
        super().__init__(action=action, data=data, status=status, headers=headers, **kwargs)

    @property
    def ok(self) -> bool:
        """Check if the reply is okay or not.

        :return: ``True`` if the reply is okay or ``False`` otherwise.
        """
        return self.status == BrokerMessageStatus.SUCCESS

    def __lt__(self, other: Any) -> bool:
        # noinspection PyBroadException
        try:
            return isinstance(other, type(self)) and self.data < other.data
        except Exception:
            return False


class BrokerMessageStatus(IntEnum):
    """Broker Message Status class."""

    SUCCESS = 200
    ERROR = 400
    SYSTEM_ERROR = 500


class BrokerMessageStrategy(str, Enum):
    """Broker Message Strategy class"""

    UNICAST = "unicast"
    MULTICAST = "multicast"
