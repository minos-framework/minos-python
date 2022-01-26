import logging

from ....collections import (
    InMemoryBrokerQueue,
)
from .abc import (
    BrokerPublisherQueue,
)

logger = logging.getLogger(__name__)


class InMemoryBrokerPublisherQueue(InMemoryBrokerQueue, BrokerPublisherQueue):
    """In Memory Broker Publisher Queue class."""
