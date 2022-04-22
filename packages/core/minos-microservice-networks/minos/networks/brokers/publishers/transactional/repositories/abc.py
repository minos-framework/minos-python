from abc import (
    ABC,
    abstractmethod,
)
from collections.abc import (
    AsyncIterator,
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
    """TODO"""

    def select(self, transaction_uuid: UUID) -> AsyncIterator[BrokerPublisherTransactionEntry]:
        """TODO"""
        return self._select(transaction_uuid)

    @abstractmethod
    def _select(self, transaction_uuid: UUID) -> AsyncIterator[BrokerPublisherTransactionEntry]:
        raise NotImplementedError

    async def submit(self, entry: BrokerPublisherTransactionEntry) -> None:
        """TODO"""
        await self._submit(entry)

    @abstractmethod
    async def _submit(self, entry: BrokerPublisherTransactionEntry) -> None:
        raise NotImplementedError

    async def delete_batch(self, transaction_uuid: UUID) -> None:
        """TODO"""
        await self._delete_batch(transaction_uuid)

    @abstractmethod
    async def _delete_batch(self, transaction_uuid: UUID) -> None:
        raise NotImplementedError
