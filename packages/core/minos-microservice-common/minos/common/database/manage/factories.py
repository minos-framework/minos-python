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
    """Manage Database Operation Factory base class."""

    @abstractmethod
    def build_create(self, database: str) -> DatabaseOperation:
        """Build the database operation to create a database.

        :param database: The new database's name.
        :return: A ``DatabaseOperation``.
        """

    @abstractmethod
    def build_delete(self, database: str) -> DatabaseOperation:
        """Build the database operation to create a database.

        :param database: The name of the database to be deleted.
        :return: A ``DatabaseOperation``.
        """


# noinspection SqlNoDataSourceInspection
class AiopgManageDatabaseOperationFactory(ManageDatabaseOperationFactory):
    """Aiopg Manage Database Operation Factory class."""

    def build_create(self, database: str) -> DatabaseOperation:
        """Build the database operation to create a database.

        :param database: The new database's name.
        :return: A ``DatabaseOperation``.
        """
        return AiopgDatabaseOperation(f"CREATE DATABASE {database};")

    def build_delete(self, database: str) -> DatabaseOperation:
        """Build the database operation to create a database.

        :param database: The name of the database to be deleted.
        :return: A ``DatabaseOperation``.
        """
        return AiopgDatabaseOperation(f"DROP DATABASE IF EXISTS {database};")


AiopgDatabaseClient.register_factory(ManageDatabaseOperationFactory, AiopgManageDatabaseOperationFactory)
