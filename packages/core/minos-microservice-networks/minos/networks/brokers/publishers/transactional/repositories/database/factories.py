from abc import (
    ABC,
    abstractmethod,
)
from typing import (
    Optional,
)
from uuid import (
    UUID,
)

from minos.common import (
    DatabaseOperation,
    DatabaseOperationFactory,
)


class BrokerPublisherTransactionDatabaseOperationFactory(DatabaseOperationFactory, ABC):
    """Broker Publisher Transaction Database Operation Factory class."""

    @abstractmethod
    def build_create(self) -> DatabaseOperation:
        """Build the operation to initialize the database storage.

        :return: A ``DatabaseOperation`` instance.
        """

    @abstractmethod
    def build_query(self, transaction_uuid: Optional[UUID]) -> DatabaseOperation:
        """Build the operation to query stored messages.

        :param transaction_uuid: The identifier of the transaction, if ``None`` is provided then the messages aren't
            filtered by transaction.
        :return: A ``DatabaseOperation`` instance.
        """

    @abstractmethod
    def build_submit(self, message: bytes, transaction_uuid: UUID) -> DatabaseOperation:
        """Build the operation to submit a new message.

        :param message: The message to be submitted.
        :param transaction_uuid: The identifier of the transaction.
        :return: A ``DatabaseOperation`` instance.
        """

    @abstractmethod
    def build_delete_batch(self, transaction_uuid: UUID) -> DatabaseOperation:
        """Build the operation to delete a batch of messages by transaction.

        :param transaction_uuid: The identifier of the transaction.
        :return: A ``DatabaseOperation`` instance.
        """
