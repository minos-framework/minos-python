from collections import (
    defaultdict,
)
from typing import (
    AsyncIterator,
)
from uuid import (
    UUID,
)

from ..entries import (
    BrokerPublisherTransactionEntry,
)
from .abc import (
    BrokerPublisherTransactionRepository,
)


class InMemoryBrokerPublisherTransactionRepository(BrokerPublisherTransactionRepository):
    """TODO"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._storage = defaultdict(list)

    async def _select(self, transaction_uuid: UUID) -> AsyncIterator[BrokerPublisherTransactionEntry]:
        for entry in self._storage[transaction_uuid]:
            yield entry

    async def _submit(self, entry: BrokerPublisherTransactionEntry) -> None:
        self._storage[entry.transaction_uuid].append(entry)

    async def _delete_batch(self, transaction_uuid: UUID) -> None:
        self._storage.pop(transaction_uuid)
