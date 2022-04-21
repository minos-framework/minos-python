from collections.abc import (
    Iterable,
)

from minos.common import (
    DatabaseOperation,
)
from minos.common.testing import (
    MockedDatabaseOperation,
)

from ....brokers import (
    BrokerQueueDatabaseOperationFactory,
)


class MockedBrokerQueueDatabaseOperationFactory(BrokerQueueDatabaseOperationFactory):
    """For testing purposes."""

    def build_create(self) -> DatabaseOperation:
        """For testing purposes"""
        return MockedDatabaseOperation("create_queue_table")

    def build_mark_processed(self, id_: int) -> DatabaseOperation:
        """For testing purposes"""
        return MockedDatabaseOperation("update_not_processed")

    def build_delete(self, id_: int) -> DatabaseOperation:
        """For testing purposes"""
        return MockedDatabaseOperation("delete_processed")

    def build_mark_processing(self, ids: Iterable[int]) -> DatabaseOperation:
        """For testing purposes"""
        return MockedDatabaseOperation("mark_processing")

    def build_count(self, retry: int, *args, **kwargs) -> DatabaseOperation:
        """For testing purposes"""
        return MockedDatabaseOperation("count_not_processed")

    def build_submit(self, topic: str, data: bytes) -> DatabaseOperation:
        """For testing purposes"""
        return MockedDatabaseOperation("insert")

    def build_query(self, retry: int, records: int, *args, **kwargs) -> DatabaseOperation:
        """For testing purposes"""
        return MockedDatabaseOperation("select_not_processed")
