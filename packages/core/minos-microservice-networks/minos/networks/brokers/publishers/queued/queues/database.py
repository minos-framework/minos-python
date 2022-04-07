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


class DatabaseBrokerPublisherQueue(DatabaseBrokerQueue, BrokerPublisherQueue):
    """PostgreSql Broker Publisher Queue class."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, operation_factory_cls=BrokerPublisherQueueDatabaseOperationFactory, **kwargs)


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


AiopgDatabaseClient.register_factory(
    BrokerPublisherQueueDatabaseOperationFactory, AiopgBrokerPublisherQueueDatabaseOperationFactory
)
