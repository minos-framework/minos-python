from collections import (
    defaultdict,
)
from itertools import (
    chain,
)
from typing import (
    AsyncIterator,
    Optional,
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
    """In Memory Broker Publisher Transaction Repository class."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._storage = defaultdict(list)

    async def _select(
        self, transaction_uuid: Optional[UUID], **kwargs
    ) -> AsyncIterator[BrokerPublisherTransactionEntry]:
        if transaction_uuid is None:
            iterable = chain.from_iterable(self._storage.values())
        else:
            iterable = self._storage[transaction_uuid]
        for entry in iterable:
            yield entry

    async def _submit(self, entry: BrokerPublisherTransactionEntry) -> None:
        self._storage[entry.transaction_uuid].append(entry)

    async def _delete_batch(self, transaction_uuid: UUID) -> None:
        self._storage.pop(transaction_uuid)
