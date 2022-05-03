from uuid import (
    UUID,
)

from minos.common import (
    DatabaseOperation,
)
from minos.common.testing import (
    MockedDatabaseClient,
    MockedDatabaseOperation,
)

from ....brokers import (
    BrokerSubscriberDuplicateValidatorDatabaseOperationFactory,
)


class MockedBrokerSubscriberDuplicateValidatorDatabaseOperationFactory(
    BrokerSubscriberDuplicateValidatorDatabaseOperationFactory
):
    """For testing purposes"""

    def build_create(self) -> DatabaseOperation:
        """For testing purposes"""
        return MockedDatabaseOperation("create_table")

    def build_submit(self, topic: str, uuid: UUID) -> DatabaseOperation:
        """For testing purposes"""
        return MockedDatabaseOperation("insert_row")


MockedDatabaseClient.set_factory(
    BrokerSubscriberDuplicateValidatorDatabaseOperationFactory,
    MockedBrokerSubscriberDuplicateValidatorDatabaseOperationFactory,
)
