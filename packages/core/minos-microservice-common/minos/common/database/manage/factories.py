from abc import (
    ABC,
    abstractmethod,
)

from ..clients import (
    AiopgDatabaseClient,
)
from ..operations import (
    AiopgDatabaseOperation,
    DatabaseOperation,
    DatabaseOperationFactory,
)


class ManageDatabaseOperationFactory(DatabaseOperationFactory, ABC):
    """TODO"""

    @abstractmethod
    def build_create(
        self,
        database: str,
    ) -> DatabaseOperation:
        """TODO"""

    @abstractmethod
    def build_delete(self, database: str) -> DatabaseOperation:
        """TODO"""


# noinspection SqlNoDataSourceInspection
class AiopgManageDatabaseOperationFactory(ManageDatabaseOperationFactory):
    """TODO"""

    def build_create(self, database: str) -> DatabaseOperation:
        """TODO"""
        return AiopgDatabaseOperation(f"CREATE DATABASE {database};")

    def build_delete(self, database: str) -> DatabaseOperation:
        """TODO"""
        return AiopgDatabaseOperation(f"DROP DATABASE IF EXISTS {database};")


AiopgDatabaseClient.register_factory(ManageDatabaseOperationFactory, AiopgManageDatabaseOperationFactory)
