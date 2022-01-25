import logging

from ....collections import (
    InMemoryBrokerRepository,
)
from .abc import (
    BrokerSubscriberRepository,
)

logger = logging.getLogger(__name__)


class InMemoryBrokerSubscriberRepository(InMemoryBrokerRepository, BrokerSubscriberRepository):
    """In Memory Broker Subscriber Repository class."""
