from __future__ import (
    annotations,
)

import logging
from abc import (
    ABC,
)

from ....collections import (
    BrokerQueueDatabaseOperationFactory,
    DatabaseBrokerQueue,
)
from .abc import (
    BrokerPublisherQueue,
)

logger = logging.getLogger(__name__)


class BrokerPublisherQueueDatabaseOperationFactory(BrokerQueueDatabaseOperationFactory, ABC):
    """Broker Publisher Queue Database Operation Factory class."""


class DatabaseBrokerPublisherQueue(
    DatabaseBrokerQueue[BrokerPublisherQueueDatabaseOperationFactory], BrokerPublisherQueue
):
    """Database Broker Publisher Queue class."""
