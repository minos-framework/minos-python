from abc import (
    ABC,
    abstractmethod,
)

from .operations import (
    DatabaseOperation,
    DatabaseOperationFactory,
)


class ManagementDatabaseOperationFactory(DatabaseOperationFactory, ABC):
    """Management Database Operation Factory base class."""

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
