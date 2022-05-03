from ....database import (
    DatabaseOperation,
    LockDatabaseOperationFactory,
)
from ..clients import (
    MockedDatabaseClient,
)
from ..operations import (
    MockedDatabaseOperation,
)


class MockedLockDatabaseOperationFactory(LockDatabaseOperationFactory):
    """For testing purposes"""

    def build_acquire(self, hashed_key: int) -> DatabaseOperation:
        """For testing purposes"""
        return MockedDatabaseOperation("acquire")

    def build_release(self, hashed_key: int) -> DatabaseOperation:
        """For testing purposes"""
        return MockedDatabaseOperation("release")


MockedDatabaseClient.set_factory(LockDatabaseOperationFactory, MockedLockDatabaseOperationFactory)
