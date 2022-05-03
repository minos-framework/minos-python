from __future__ import (
    annotations,
)

from abc import (
    ABC,
    abstractmethod,
)
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
    Inject,
    Injectable,
    Lock,
    LockPool,
    NotProvidedException,
    PoolFactory,
    SetupMixin,
)

from ...exceptions import (
    TransactionNotFoundException,
)
from ..entries import (
    TransactionEntry,
    TransactionStatus,
)


@Injectable("transaction_repository")
class TransactionRepository(ABC, SetupMixin):
    """Transaction Repository base class."""

    @Inject()
    def __init__(
        self, lock_pool: Optional[LockPool] = None, pool_factory: Optional[PoolFactory] = None, *args, **kwargs
    ):
        super().__init__(*args, **kwargs)

        if lock_pool is None and pool_factory is not None:
            lock_pool = pool_factory.get_pool("lock")

        if lock_pool is None:
            raise NotProvidedException("A lock pool instance is required.")

        self._lock_pool = lock_pool

    async def submit(self, transaction: TransactionEntry) -> TransactionEntry:
        """Submit a new or updated transaction to store it on the repository.

        :param transaction: The transaction to be stored.
        :return: This method does not return anything.
        """
        return await self._submit(transaction)

    @abstractmethod
    async def _submit(self, transaction: TransactionEntry) -> TransactionEntry:
        raise NotImplementedError

    # noinspection PyUnusedLocal
    async def get(self, uuid: UUID, **kwargs) -> TransactionEntry:
        """Get a ``TransactionEntry`` from its identifier.

        :param uuid: Identifier of the ``RootEntity``.
        :param kwargs: Additional named arguments.
        :return: The ``TransactionEntry`` instance.
        """
        try:
            return await self.select(uuid=uuid).__anext__()
        except StopAsyncIteration:
            raise TransactionNotFoundException(f"Transaction identified by {uuid!r} does not exist.")

    async def select(
        self,
        uuid: Optional[UUID] = None,
        uuid_ne: Optional[UUID] = None,
        uuid_in: Optional[tuple[UUID, ...]] = None,
        destination_uuid: Optional[UUID] = None,
        status: Optional[TransactionStatus] = None,
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
        """Get a transaction from the repository.

        :param uuid: Transaction identifier equal to the given value.
        :param uuid_ne: Transaction identifier not equal to the given value
        :param uuid_in: Transaction identifier within the given values.
        :param destination_uuid: Destination Transaction identifier equal to the given value.
        :param status: Transaction status equal to the given value.
        :param status_in: Transaction status within the given values
        :param event_offset: Event offset equal to the given value.
        :param event_offset_lt: Event Offset lower than the given value
        :param event_offset_gt: Event Offset greater than the given value
        :param event_offset_le: Event Offset lower or equal to the given value
        :param event_offset_ge: Event Offset greater or equal to the given value
        :param updated_at: Updated at equal to the given value.
        :param updated_at_lt: Updated at lower than the given value.
        :param updated_at_gt: Updated at greater than the given value.
        :param updated_at_le: Updated at lower or equal to the given value.
        :param updated_at_ge: Updated at greater or equal to the given value.
        :param kwargs: Additional named arguments.
        :return: An asynchronous iterator.
        """
        generator = self._select(
            uuid=uuid,
            uuid_ne=uuid_ne,
            uuid_in=uuid_in,
            destination_uuid=destination_uuid,
            status=status,
            status_in=status_in,
            event_offset=event_offset,
            event_offset_lt=event_offset_lt,
            event_offset_gt=event_offset_gt,
            event_offset_le=event_offset_le,
            event_offset_ge=event_offset_ge,
            updated_at=updated_at,
            updated_at_lt=updated_at_lt,
            updated_at_gt=updated_at_gt,
            updated_at_le=updated_at_le,
            updated_at_ge=updated_at_ge,
            **kwargs,
        )
        # noinspection PyTypeChecker
        async for entry in generator:
            yield entry

    @abstractmethod
    async def _select(self, **kwargs) -> AsyncIterator[TransactionEntry]:
        raise NotImplementedError

    def write_lock(self) -> Lock:
        """Get write lock.

        :return: An asynchronous context manager.
        """
        return self._lock_pool.acquire("aggregate_transaction_write_lock")
