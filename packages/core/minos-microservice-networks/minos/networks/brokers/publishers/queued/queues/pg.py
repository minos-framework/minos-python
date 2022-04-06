from __future__ import (
    annotations,
)

import logging
from abc import (
    ABC,
)
from typing import (
    Optional,
)

from ....collections import (
    AiopgBrokerQueueDatabaseOperationFactory,
    DatabaseBrokerQueue,
)
from ....collections.queues.database.factories.abc import (
    BrokerQueueDatabaseOperationFactory,
)
from .abc import (
    BrokerPublisherQueue,
)

logger = logging.getLogger(__name__)


class DatabaseBrokerPublisherQueue(DatabaseBrokerQueue, BrokerPublisherQueue):
    """PostgreSql Broker Publisher Queue class."""

    def __init__(self, *args, query_factory: Optional[BrokerPublisherQueueDatabaseOperationFactory] = None, **kwargs):
        if query_factory is None:
            query_factory = AiopgBrokerPublisherQueueDatabaseOperationFactory()
        super().__init__(*args, query_factory=query_factory, **kwargs)


class BrokerPublisherQueueDatabaseOperationFactory(BrokerQueueDatabaseOperationFactory, ABC):
    """TODO"""


class AiopgBrokerPublisherQueueDatabaseOperationFactory(
    BrokerPublisherQueueDatabaseOperationFactory, AiopgBrokerQueueDatabaseOperationFactory
):
    """PostgreSql Broker Publisher Queue Query Factory class."""

    def build_table_name(self) -> str:
        """Get the table name.

        :return: A ``str`` value.
        """
        return "broker_publisher_queue"
