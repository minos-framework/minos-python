from abc import (
    ABC,
    abstractmethod,
)
from collections.abc import (
    AsyncIterator,
)
from typing import (
    Optional,
)
from uuid import (
    UUID,
)

from minos.common import (
    SetupMixin,
)

from ..entries import (
    BrokerPublisherTransactionEntry,
)


class BrokerPublisherTransactionRepository(ABC, SetupMixin):
    """Broker Publisher Transaction Repository class."""

    def select(
        self, transaction_uuid: Optional[UUID] = None, **kwargs
    ) -> AsyncIterator[BrokerPublisherTransactionEntry]:
        """Perform a query on the repository.

        :param transaction_uuid: The identifier of the transaction. If ``None`` is provided then the entries are not
            filtered by transaction.
        :param kwargs: Additional named arguments.
        :return: An ``AsyncIterator`` of ``BrokerPublisherTransactionEntry``.
        """
        return self._select(transaction_uuid=transaction_uuid, **kwargs)

    @abstractmethod
    def _select(self, transaction_uuid: Optional[UUID], **kwargs) -> AsyncIterator[BrokerPublisherTransactionEntry]:
        raise NotImplementedError

    async def submit(self, entry: BrokerPublisherTransactionEntry) -> None:
        """Submit a new entry to the repository.

        :param entry: The entry to be submitted.
        :return: This method does not return anything.
        """
        await self._submit(entry=entry)

    @abstractmethod
    async def _submit(self, entry: BrokerPublisherTransactionEntry) -> None:
        raise NotImplementedError

    async def delete_batch(self, transaction_uuid: UUID) -> None:
        """Delete a batch of entries by transaction.

        :param transaction_uuid: The identifier of the transaction.
        :return: This method does not return anything.
        """
        await self._delete_batch(transaction_uuid=transaction_uuid)

    @abstractmethod
    async def _delete_batch(self, transaction_uuid: UUID) -> None:
        raise NotImplementedError
