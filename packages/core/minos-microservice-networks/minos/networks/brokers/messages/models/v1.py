from __future__ import (
    annotations,
)

import logging
import warnings
from enum import (
    Enum,
)
from functools import (
    total_ordering,
)
from typing import (
    Any,
    Optional,
)
from uuid import (
    UUID,
    uuid4,
)

from minos.common import (
    DeclarativeModel,
)

from .abc import (
    BrokerMessage,
)

logger = logging.getLogger(__name__)


@total_ordering
class BrokerMessageV1(BrokerMessage, DeclarativeModel):
    """Broker Message V1 class."""

    topic: str
    identifier: UUID
    reply_topic: Optional[str]
    strategy: BrokerMessageV1Strategy  # FIXME: Remove this attribute!

    payload: BrokerMessageV1Payload

    def __init__(
        self,
        topic: str,
        payload: BrokerMessageV1Payload,
        *,
        identifier: Optional[UUID] = None,
        strategy: Optional[BrokerMessageV1Strategy] = None,
        **kwargs,
    ):
        if identifier is None:
            identifier = uuid4()
        if strategy is None:
            strategy = BrokerMessageV1Strategy.UNICAST

        super().__init__(topic=topic, identifier=identifier, strategy=strategy, payload=payload, **kwargs)

    # noinspection PyPropertyDefinition
    @classmethod
    @property
    def version(cls) -> int:
        """Get the version of the message.

        :return: A strictly positive ``int`` value.
        """
        return 1

    @property
    def topic(self) -> str:
        """Get the topic of the message.

        :return: A ``str`` value.
        """
        return self.fields["topic"].value

    @property
    def identifier(self) -> UUID:
        """Get the identifier of the message.

        :return: An ``UUID`` instance.
        """
        return self.fields["identifier"].value

    @property
    def reply_topic(self) -> Optional[str]:
        """Get the reply topic of the message if there is someone.

        :return: A ``str`` value or ``None``.
        """
        return self.fields["reply_topic"].value

    def set_reply_topic(self, value: Optional[str]) -> None:
        """Set the message's reply topic.

        :param value: A ``str`` value or ``None``.
        :return: This method does not return anything.
        """
        self.fields["reply_topic"].value = value

    @property
    def content(self) -> Any:
        """Get the content of the message.

        :return: Any value.
        """
        return self.payload.content

    @property
    def ok(self) -> bool:
        """Check if the message is okay or not.

        :return: ``True`` if the message is okay or ``False`` otherwise.
        """
        return self.payload.ok

    @property
    def status(self) -> int:
        """Get the payload status.

        :return: A ``BrokerMessageV1Status`` instance.
        """
        return self.payload.status

    @property
    def headers(self) -> dict[str, str]:
        """Get the payload headers.

        :return: A `dict` with `str` keys and `str` values.
        """
        return self.payload.headers

    @property
    def data(self) -> Any:
        """Get the payload content.

        :return: Any value.
        """
        warnings.warn("The `BrokerMessage.data` attribute has been deprecated", DeprecationWarning)
        return self.payload.content

    def __lt__(self, other: Any) -> bool:
        # noinspection PyBroadException
        return isinstance(other, type(self)) and self.payload < other.payload


@total_ordering
class BrokerMessageV1Payload(DeclarativeModel):
    """Broker Message Payload class."""

    content: Any
    status: BrokerMessageV1Status
    headers: dict[str, str]

    def __init__(
        self, content: Any = None, headers: Optional[dict[str, str]] = None, status: Optional[int] = None, **kwargs
    ):
        if headers is None:
            headers = dict()
        if status is None:
            status = BrokerMessageV1Status.SUCCESS
        super().__init__(content=content, status=status, headers=headers, **kwargs)

    @property
    def ok(self) -> bool:
        """Check if the message is okay or not.

        :return: ``True`` if the message is okay or ``False`` otherwise.
        """
        return self.status == BrokerMessageV1Status.SUCCESS

    @property
    def data(self) -> Any:
        """Get the content.

        :return: Any value.
        """
        warnings.warn("The `BrokerMessage.data` attribute has been deprecated", DeprecationWarning)
        return self.content

    def __lt__(self, other: Any) -> bool:
        # noinspection PyBroadException
        try:
            return isinstance(other, type(self)) and self.content < other.content
        except Exception:
            return False


class BrokerMessageV1Status(int, Enum):
    """Broker Message Status v1 class."""

    SUCCESS = 200
    ERROR = 400
    SYSTEM_ERROR = 500
    UNKNOWN = 600

    @classmethod
    def _missing_(cls, value: Any) -> BrokerMessageV1Status:
        logger.warning(f"The given status value is unknown: {value}")
        return cls.UNKNOWN


class BrokerMessageV1Strategy(str, Enum):
    """Broker Message Strategy class"""

    UNICAST = "unicast"
    MULTICAST = "multicast"
