from __future__ import (
    annotations,
)

import logging

from ....collections import (
    InMemoryBrokerQueue,
)
from .abc import (
    BrokerSubscriberQueue,
    BrokerSubscriberQueueBuilder,
)

logger = logging.getLogger(__name__)


class InMemoryBrokerSubscriberQueue(InMemoryBrokerQueue, BrokerSubscriberQueue):
    """In Memory Broker Subscriber Queue class."""


class InMemoryBrokerSubscriberQueueBuilder(BrokerSubscriberQueueBuilder):
    """In Memory Broker Subscriber Queue Builder class."""

    def build(self) -> BrokerSubscriberQueue:
        """Build the instance.

        :return: An ``InMemoryBrokerSubscriberQueue`` instance.
        """
        return InMemoryBrokerSubscriberQueue(**self.kwargs)
