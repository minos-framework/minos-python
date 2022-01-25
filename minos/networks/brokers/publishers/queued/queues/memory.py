import logging

from ....collections import (
    InMemoryBrokerRepository,
)
from .abc import (
    BrokerPublisherQueue,
)

logger = logging.getLogger(__name__)


class InMemoryBrokerPublisherQueue(InMemoryBrokerRepository, BrokerPublisherQueue):
    """In Memory Broker Publisher Queue class."""
