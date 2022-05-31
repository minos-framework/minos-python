from sqlalchemy import (
    text,
)

from minos.common import (
    ComposedDatabaseOperation,
    DatabaseOperation,
    ManagementDatabaseOperationFactory,
)

from ...clients import (
    SqlAlchemyDatabaseClient,
)
from ...operations import (
    SqlAlchemyDatabaseOperation,
)


# noinspection SqlNoDataSourceInspection,SqlDialectInspection
class SqlAlchemyManagementDatabaseOperationFactory(ManagementDatabaseOperationFactory):
    """SqlAlchemy Manage Database Operation Factory class."""

    def build_create(self, database: str) -> DatabaseOperation:
        """Build the database operation to create a database.

        :param database: The new database's name.
        :return: A ``DatabaseOperation``.
        """
        return ComposedDatabaseOperation(
            [
                SqlAlchemyDatabaseOperation(text("COMMIT")),
                SqlAlchemyDatabaseOperation(text(f"CREATE DATABASE {database};"), stream=False),
            ]
        )

    def build_delete(self, database: str) -> DatabaseOperation:
        """Build the database operation to create a database.

        :param database: The name of the database to be deleted.
        :return: A ``DatabaseOperation``.
        """
        return ComposedDatabaseOperation(
            [
                SqlAlchemyDatabaseOperation(text("COMMIT")),
                SqlAlchemyDatabaseOperation(text(f"DROP DATABASE IF EXISTS {database};"), stream=False),
            ]
        )


SqlAlchemyDatabaseClient.set_factory(ManagementDatabaseOperationFactory, SqlAlchemyManagementDatabaseOperationFactory)
