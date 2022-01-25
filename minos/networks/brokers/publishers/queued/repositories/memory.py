import logging

from ....collections import (
    InMemoryBrokerRepository,
)
from .abc import (
    BrokerPublisherRepository,
)

logger = logging.getLogger(__name__)


class InMemoryBrokerPublisherRepository(InMemoryBrokerRepository, BrokerPublisherRepository):
    """In Memory Broker Publisher Repository class."""
