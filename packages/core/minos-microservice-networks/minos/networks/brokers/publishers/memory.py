from __future__ import (
    annotations,
)

import logging

from ..messages import (
    BrokerMessage,
)
from .abc import (
    BrokerPublisher,
)

logger = logging.getLogger(__name__)


class InMemoryBrokerPublisher(BrokerPublisher):
    """In Memory Broker Publisher class."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._messages = list()

    async def _send(self, message: BrokerMessage) -> None:
        """Send a message.

        :param message: The message to be sent.
        :return: This method does not return anything.
        """
        self._messages.append(message)

    @property
    def messages(self) -> list[BrokerMessage]:
        """The sequence of sent messages.

        :return: A list of ``BrokerMessage`` entries.
        """
        return self._messages
