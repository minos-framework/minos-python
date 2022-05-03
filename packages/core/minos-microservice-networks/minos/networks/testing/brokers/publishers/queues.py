from minos.common.testing import (
    MockedDatabaseClient,
)
from minos.networks import (
    BrokerPublisherQueueDatabaseOperationFactory,
)

from ..collections import (
    MockedBrokerQueueDatabaseOperationFactory,
)


class MockedBrokerPublisherQueueDatabaseOperationFactory(
    BrokerPublisherQueueDatabaseOperationFactory, MockedBrokerQueueDatabaseOperationFactory
):
    """For testing purposes"""


MockedDatabaseClient.set_factory(
    BrokerPublisherQueueDatabaseOperationFactory, MockedBrokerPublisherQueueDatabaseOperationFactory
)
