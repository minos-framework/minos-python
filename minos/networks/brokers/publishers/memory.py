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
    """TODO"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._messages = list()

    async def send(self, message: BrokerMessage) -> None:
        """TODO

        :param message: TODO
        :return: TODO
        """
        self._messages.append(message)

    @property
    def messages(self) -> list[BrokerMessage]:
        """TODO"""
        return self._messages
