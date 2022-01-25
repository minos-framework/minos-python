import logging

from ....collections import (
    InMemoryBrokerQueue,
)
from .abc import (
    BrokerSubscriberQueue,
)

logger = logging.getLogger(__name__)


class InMemoryBrokerSubscriberQueue(InMemoryBrokerQueue, BrokerSubscriberQueue):
    """In Memory Broker Subscriber Queue class."""
