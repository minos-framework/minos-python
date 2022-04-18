from collections.abc import (
    Iterable,
)

from minos.common import (
    DatabaseOperation,
)
from minos.common.testing import (
    MockedDatabaseClient,
    MockedDatabaseOperation,
)

from ....brokers import (
    BrokerSubscriberQueueDatabaseOperationFactory,
)
from ..collections import (
    MockedBrokerQueueDatabaseOperationFactory,
)


class MockedBrokerSubscriberQueueDatabaseOperationFactory(
    BrokerSubscriberQueueDatabaseOperationFactory, MockedBrokerQueueDatabaseOperationFactory
):
    """For testing purposes"""

    def build_count(self, retry: int, topics: Iterable[str] = tuple(), *args, **kwargs) -> DatabaseOperation:
        """For testing purposes"""
        return MockedDatabaseOperation("count_not_processed")

    def build_query(
        self, retry: int, records: int, topics: Iterable[str] = tuple(), *args, **kwargs
    ) -> DatabaseOperation:
        """For testing purposes"""
        return MockedDatabaseOperation("select_not_processed")


MockedDatabaseClient.set_factory(
    BrokerSubscriberQueueDatabaseOperationFactory, MockedBrokerSubscriberQueueDatabaseOperationFactory
)
