from __future__ import (
    annotations,
)

from functools import (
    partial,
)

from sqlalchemy.engine import (
    URL,
    Connectable,
)
from sqlalchemy_utils import (
    create_database,
    database_exists,
    drop_database,
)

from minos.common import (
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
        return SqlAlchemyDatabaseOperation(partial(self._create_database, database=database))

    def build_delete(self, database: str) -> DatabaseOperation:
        """Build the database operation to create a database.

        :param database: The name of the database to be deleted.
        :return: A ``DatabaseOperation``.
        """
        return SqlAlchemyDatabaseOperation(partial(self._drop_database, database=database))

    @classmethod
    def _create_database(cls, connectable: Connectable, database: str) -> None:
        url = cls._overwrite_url(connectable.engine.url, database)
        if not database_exists(url):
            create_database(url)

    @classmethod
    def _drop_database(cls, connectable: Connectable, database: str) -> None:
        url = cls._overwrite_url(connectable.engine.url, database)
        if database_exists(url):
            drop_database(url)

    @staticmethod
    def _overwrite_url(url: URL, database: str) -> URL:
        return url.set(database=database, drivername=url.drivername.split("+")[0])


SqlAlchemyDatabaseClient.set_factory(ManagementDatabaseOperationFactory, SqlAlchemyManagementDatabaseOperationFactory)
