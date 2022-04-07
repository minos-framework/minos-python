from abc import (
    ABC,
    abstractmethod,
)
from collections.abc import (
    Iterable,
)

from minos.common import (
    DatabaseOperation,
    DatabaseOperationFactory,
)


# noinspection SqlResolve,SqlNoDataSourceInspection,SqlNoDataSourceInspection,SqlResolve
class BrokerQueueDatabaseOperationFactory(DatabaseOperationFactory, ABC):
    """Broker Queue Database Operation Factory class."""

    @abstractmethod
    def build_create_table(self) -> DatabaseOperation:
        """Build the "create table" query.

        :return: A ``SQL`` instance.
        """

    @abstractmethod
    def build_update_not_processed(self, id_: int) -> DatabaseOperation:
        """Build the "update not processed" query.

        :return: A ``SQL`` instance.
        """

    @abstractmethod
    def build_delete_processed(self, id_: int) -> DatabaseOperation:
        """Build the "delete processed" query.

        :return: A ``SQL`` instance.
        """

    @abstractmethod
    def build_mark_processing(self, ids: Iterable[int]) -> DatabaseOperation:
        """

        :return: A ``SQL`` instance.
        """

    @abstractmethod
    def build_count_not_processed(self, retry: int, *args, **kwargs) -> DatabaseOperation:
        """Build the "count not processed" query.

        :return:
        """

    @abstractmethod
    def build_insert(self, topic: str, data: bytes) -> DatabaseOperation:
        """Build the "insert" query.

        :return: A ``SQL`` instance.
        """

    @abstractmethod
    def build_select_not_processed(self, retry: int, records: int, *args, **kwargs) -> DatabaseOperation:
        """Build the "select not processed" query.

        :return: A ``SQL`` instance.
        """
