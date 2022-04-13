from minos.common import (
    DatabaseOperation,
    ManagementDatabaseOperationFactory,
)

from ...clients import (
    AiopgDatabaseClient,
)
from ...operations import (
    AiopgDatabaseOperation,
)


# noinspection SqlNoDataSourceInspection
class AiopgManagementDatabaseOperationFactory(ManagementDatabaseOperationFactory):
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


AiopgDatabaseClient.set_factory(ManagementDatabaseOperationFactory, AiopgManagementDatabaseOperationFactory)
