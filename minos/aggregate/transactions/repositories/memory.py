from datetime import (
    datetime,
)
from typing import (
    AsyncIterator,
    Optional,
)
from uuid import (
    UUID,
)

from minos.common import (
    current_datetime,
)

from ...exceptions import (
    TransactionRepositoryConflictException,
)
from ..entries import (
    TransactionEntry,
)
from ..entries import TransactionStatus as s
from .abc import (
    TransactionRepository,
)


class InMemoryTransactionRepository(TransactionRepository):
    """In Memory Transaction Repository class."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._storage = dict()

    async def _submit(self, transaction: TransactionEntry) -> TransactionEntry:
        transaction.updated_at = current_datetime()

        if transaction.uuid in self._storage:
            status = self._storage[transaction.uuid].status
            if (
                (status == s.PENDING and transaction.status not in (s.PENDING, s.RESERVING, s.REJECTED))
                or (status == s.RESERVING and transaction.status not in (s.RESERVED, s.REJECTED))
                or (status == s.RESERVED and transaction.status not in (s.COMMITTING, s.REJECTED))
                or (status == s.COMMITTING and transaction.status not in (s.COMMITTED,))
                or (status == s.COMMITTED)
                or (status == s.REJECTED)
            ):
                raise TransactionRepositoryConflictException(
                    f"{transaction!r} status is invalid respect to the previous one."
                )

        self._storage[transaction.uuid] = TransactionEntry(
            uuid=transaction.uuid,
            destination_uuid=transaction.destination_uuid,
            status=transaction.status,
            event_offset=transaction.event_offset,
            updated_at=transaction.updated_at,
            event_repository=transaction._event_repository,
            transaction_repository=transaction._transaction_repository,
        )

        return transaction

    async def _select(
        self,
        uuid: Optional[UUID] = None,
        uuid_ne: Optional[UUID] = None,
        uuid_in: Optional[tuple[UUID, ...]] = None,
        destination_uuid: Optional[UUID] = None,
        status: Optional[s] = None,
        status_in: Optional[tuple[str, ...]] = None,
        event_offset: Optional[int] = None,
        event_offset_lt: Optional[int] = None,
        event_offset_gt: Optional[int] = None,
        event_offset_le: Optional[int] = None,
        event_offset_ge: Optional[int] = None,
        updated_at: Optional[datetime] = None,
        updated_at_lt: Optional[datetime] = None,
        updated_at_gt: Optional[datetime] = None,
        updated_at_le: Optional[datetime] = None,
        updated_at_ge: Optional[datetime] = None,
        **kwargs,
    ) -> AsyncIterator[TransactionEntry]:

        # noinspection DuplicatedCode
        def _fn_filter(transaction: TransactionEntry) -> bool:
            if uuid is not None and uuid != transaction.uuid:
                return False
            if uuid_ne is not None and uuid_ne == transaction.uuid:
                return False
            if uuid_in is not None and transaction.uuid not in uuid_in:
                return False
            if destination_uuid is not None and destination_uuid != transaction.destination_uuid:
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
            if updated_at is not None and updated_at != transaction.updated_at:
                return False
            if updated_at_lt is not None and updated_at_lt <= transaction.updated_at:
                return False
            if updated_at_gt is not None and updated_at_gt >= transaction.updated_at:
                return False
            if updated_at_le is not None and updated_at_le < transaction.updated_at:
                return False
            if updated_at_ge is not None and updated_at_ge > transaction.updated_at:
                return False
            return True

        iterable = iter(self._storage.values())
        iterable = filter(_fn_filter, iterable)
        for item in iterable:
            yield item
