from abc import (
    ABC,
    abstractmethod,
)
from uuid import (
    UUID,
)

from minos.common import (
    DatabaseOperation,
    DatabaseOperationFactory,
)


class BrokerPublisherTransactionDatabaseOperationFactory(DatabaseOperationFactory, ABC):
    """TODO"""

    @abstractmethod
    def build_create(self) -> DatabaseOperation:
        """TODO"""

    @abstractmethod
    def build_query(self, transaction_uuid: UUID) -> DatabaseOperation:
        """TODO"""

    @abstractmethod
    def build_submit(self, message: bytes, transaction_uuid: UUID) -> DatabaseOperation:
        """TODO"""

    @abstractmethod
    def build_delete_batch(self, transaction_uuid: UUID) -> DatabaseOperation:
        """TODO"""
