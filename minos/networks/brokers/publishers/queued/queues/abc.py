import logging
from abc import (
    ABC,
)

from ....collections import (
    BrokerQueue,
)

logger = logging.getLogger(__name__)


class BrokerPublisherQueue(BrokerQueue, ABC):
    """Broker Publisher Queue class."""
