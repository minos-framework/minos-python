import logging
from abc import (
    ABC,
)

from ....collections import (
    BrokerRepository,
)

logger = logging.getLogger(__name__)


class BrokerPublisherRepository(BrokerRepository, ABC):
    """Broker Publisher Repository class."""
