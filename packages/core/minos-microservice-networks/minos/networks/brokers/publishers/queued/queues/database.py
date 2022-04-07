from __future__ import (
    annotations,
)

import logging
from abc import (
    ABC,
)

from minos.common import (
    AiopgDatabaseClient,
)

from ....collections import (
    AiopgBrokerQueueDatabaseOperationFactory,
    BrokerQueueDatabaseOperationFactory,
    DatabaseBrokerQueue,
)
from .abc import (
    BrokerPublisherQueue,
)

logger = logging.getLogger(__name__)


class BrokerPublisherQueueDatabaseOperationFactory(BrokerQueueDatabaseOperationFactory, ABC):
    """Broker Publisher Queue Database Operation Factory class."""


class AiopgBrokerPublisherQueueDatabaseOperationFactory(
    BrokerPublisherQueueDatabaseOperationFactory, AiopgBrokerQueueDatabaseOperationFactory
):
    """Aiopg Broker Publisher Queue Query Factory class."""

    def build_table_name(self) -> str:
        """Get the table name.

        :return: A ``str`` value.
        """
        return "broker_publisher_queue"


AiopgDatabaseClient.register_factory(
    BrokerPublisherQueueDatabaseOperationFactory, AiopgBrokerPublisherQueueDatabaseOperationFactory
)


class DatabaseBrokerPublisherQueue(
    DatabaseBrokerQueue[BrokerPublisherQueueDatabaseOperationFactory], BrokerPublisherQueue
):
    """Database Broker Publisher Queue class."""
