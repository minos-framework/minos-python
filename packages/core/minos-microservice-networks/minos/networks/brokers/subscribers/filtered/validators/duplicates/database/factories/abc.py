from abc import (
    ABC,
    abstractmethod,
)
from uuid import (
    UUID,
)

from minos.common import (
    DatabaseOperation,
)


# noinspection SqlNoDataSourceInspection,SqlResolve
class BrokerSubscriberDuplicateValidatorDatabaseOperationFactory(ABC):
    """TODO"""

    @abstractmethod
    def build_create_table(self) -> DatabaseOperation:
        """Build the "create table" query.

        :return: A ``SQL`` instance.
        """

    @abstractmethod
    def build_insert_row(self, topic: str, uuid: UUID) -> DatabaseOperation:
        """Build the "insert row" query.

        :return: A ``SQL`` instance.
        """
