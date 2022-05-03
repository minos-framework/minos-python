from ....database import (
    DatabaseOperation,
    ManagementDatabaseOperationFactory,
)
from ..clients import (
    MockedDatabaseClient,
)
from ..operations import (
    MockedDatabaseOperation,
)


class MockedManagementDatabaseOperationFactory(ManagementDatabaseOperationFactory):
    """For testing purposes"""

    def build_create(self, database: str) -> DatabaseOperation:
        """For testing purposes"""
        return MockedDatabaseOperation("create")

    def build_delete(self, database: str) -> DatabaseOperation:
        """For testing purposes"""
        return MockedDatabaseOperation("delete")


MockedDatabaseClient.set_factory(ManagementDatabaseOperationFactory, MockedManagementDatabaseOperationFactory)
