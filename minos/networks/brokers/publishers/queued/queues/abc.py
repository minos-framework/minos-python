import logging
from abc import (
    ABC,
)

from ....collections import (
    BrokerRepository,
)

logger = logging.getLogger(__name__)


class BrokerPublisherQueue(BrokerRepository, ABC):
    """Broker Publisher Queue class."""
