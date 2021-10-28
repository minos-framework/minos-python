from typing import (
    AsyncIterator,
    Optional,
)
from uuid import (
    UUID,
)

from ...datetime import (
    current_datetime,
)
from ..models import (
    Transaction,
    TransactionStatus,
)
from .abc import (
    TransactionRepository,
)


class InMemoryTransactionRepository(TransactionRepository):
    """In Memory Transaction Repository class."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._storage = dict()

    async def _submit(self, transaction: Transaction) -> Transaction:
        transaction.updated_at = current_datetime()
        self._storage[transaction.uuid] = transaction
        return transaction

    async def _select(
        self,
        uuid: Optional[UUID] = None,
        uuid_in: Optional[tuple[UUID, ...]] = None,
        status: Optional[TransactionStatus] = None,
        status_in: Optional[tuple[str, ...]] = None,
        event_offset: Optional[int] = None,
        event_offset_lt: Optional[int] = None,
        event_offset_gt: Optional[int] = None,
        event_offset_le: Optional[int] = None,
        event_offset_ge: Optional[int] = None,
        **kwargs
    ) -> AsyncIterator[Transaction]:

        # noinspection DuplicatedCode
        def _fn_filter(transaction: Transaction) -> bool:
            if uuid is not None and uuid != transaction.uuid:
                return False
            if uuid_in is not None and transaction.uuid not in uuid_in:
                return False
            if status is not None and status != transaction.status:
                return False
            if status_in is not None and transaction.status not in status_in:
                return False
            if event_offset is not None and event_offset != transaction.event_offset:
                return False
            if event_offset_lt is not None and event_offset_lt <= transaction.event_offset:
                return False
            if event_offset_gt is not None and event_offset_gt >= transaction.event_offset:
                return False
            if event_offset_le is not None and event_offset_le < transaction.event_offset:
                return False
            if event_offset_ge is not None and event_offset_ge > transaction.event_offset:
                return False
            return True

        iterable = iter(self._storage.values())
        iterable = filter(_fn_filter, iterable)
        for item in iterable:
            yield item
