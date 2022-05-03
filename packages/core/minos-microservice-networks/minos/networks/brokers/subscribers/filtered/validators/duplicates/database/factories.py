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


# noinspection SqlNoDataSourceInspection,SqlResolve
class BrokerSubscriberDuplicateValidatorDatabaseOperationFactory(DatabaseOperationFactory, ABC):
    """Broker Subscriber Duplicate Validator Database Operation Factory class."""

    @abstractmethod
    def build_create(self) -> DatabaseOperation:
        """Build the "create table" query.

        :return: A ``SQL`` instance.
        """

    @abstractmethod
    def build_submit(self, topic: str, uuid: UUID) -> DatabaseOperation:
        """Build the "insert row" query.

        :return: A ``SQL`` instance.
        """
