from minos.networks import (
    BrokerPublisherQueueDatabaseOperationFactory,
)

from ....clients import (
    AiopgDatabaseClient,
)
from ..collections import (
    AiopgBrokerQueueDatabaseOperationFactory,
)


class AiopgBrokerPublisherQueueDatabaseOperationFactory(
    BrokerPublisherQueueDatabaseOperationFactory, AiopgBrokerQueueDatabaseOperationFactory
):
    """Aiopg Broker Publisher Queue Query Factory class."""

    def build_table_name(self) -> str:
        """Get the table name.

        :return: A ``str`` value.
        """
        return "broker_publisher_queue"


AiopgDatabaseClient.set_factory(
    BrokerPublisherQueueDatabaseOperationFactory, AiopgBrokerPublisherQueueDatabaseOperationFactory
)
