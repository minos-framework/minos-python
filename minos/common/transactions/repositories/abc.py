from abc import (
    ABC,
    abstractmethod,
)
from typing import (
    AsyncIterator,
    Optional,
)
from uuid import (
    UUID,
)

from dependency_injector.wiring import (
    Provide,
    inject,
)

from ...exceptions import (
    MinosLockPoolNotProvidedException,
)
from ...locks import (
    Lock,
)
from ...pools import (
    MinosPool,
)
from ...setup import (
    MinosSetup,
)
from ..entries import (
    TransactionEntry,
    TransactionStatus,
)


class TransactionRepository(ABC, MinosSetup):
    """Transaction Repository base class."""

    @inject
    def __init__(
        self, lock_pool: MinosPool[Lock] = Provide["lock_pool"], *args, **kwargs,
    ):
        super().__init__(*args, **kwargs)

        if lock_pool is None or isinstance(lock_pool, Provide):
            raise MinosLockPoolNotProvidedException("A transaction repository instance is required.")

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

    async def select(
        self,
        uuid: Optional[UUID] = None,
        uuid_ne: Optional[UUID] = None,
        uuid_in: Optional[tuple[UUID, ...]] = None,
        destination: Optional[UUID] = None,
        status: Optional[TransactionStatus] = None,
        status_in: Optional[tuple[str, ...]] = None,
        event_offset: Optional[int] = None,
        event_offset_lt: Optional[int] = None,
        event_offset_gt: Optional[int] = None,
        event_offset_le: Optional[int] = None,
        event_offset_ge: Optional[int] = None,
        **kwargs,
    ) -> AsyncIterator[TransactionEntry]:
        """Get a transaction from the repository.

        :param uuid: Transaction identifier equal to the given value.
        :param uuid_ne: Transaction identifier not equal to the given value
        :param uuid_in: Transaction identifier within the given values.
        :param destination: TODO
        :param status: Transaction status equal to the given value.
        :param status_in: Transaction status within the given values
        :param event_offset: Event offset equal to the given value.
        :param event_offset_lt: Event Offset lower than the given value
        :param event_offset_gt: Event Offset greater than the given value
        :param event_offset_le: Event Offset lower or equal to the given value
        :param event_offset_ge: Event Offset greater or equal to the given value
        :param kwargs: Additional named arguments.
        :return: An asynchronous iterator.
        """
        generator = self._select(
            uuid=uuid,
            uuid_ne=uuid_ne,
            uuid_in=uuid_in,
            destination=destination,
            status=status,
            status_in=status_in,
            event_offset=event_offset,
            event_offset_lt=event_offset_lt,
            event_offset_gt=event_offset_gt,
            event_offset_le=event_offset_le,
            event_offset_ge=event_offset_ge,
            **kwargs,
        )
        # noinspection PyTypeChecker
        async for entry in generator:
            yield entry

    @abstractmethod
    async def _select(self, **kwargs) -> AsyncIterator[TransactionEntry]:
        raise NotImplementedError

    def write_lock(self) -> Lock:
        """Get a write lock.

        :return: An asynchronous context manager.
        """
        return self._lock_pool.acquire("aggregate_transaction_write_lock")
