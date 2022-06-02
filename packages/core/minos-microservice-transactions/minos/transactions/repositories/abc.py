from __future__ import (
    annotations,
)

from abc import (
    ABC,
    abstractmethod,
)
from collections.abc import (
    Iterable,
)
from datetime import (
    datetime,
)
from typing import (
    TYPE_CHECKING,
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

from ..entries import (
    TransactionEntry,
    TransactionStatus,
)
from ..exceptions import (
    TransactionNotFoundException,
)

if TYPE_CHECKING:
    from ..mixins import (
        TransactionalMixin,
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

        self._observers = set()

    @property
    def observers(self) -> set[TransactionalMixin]:
        """Get the list of observers.

        :return: A ``list`` of ``TransactionalMixin`` entries.
        """
        return self._observers

    def register_observer(self, observer: TransactionalMixin) -> None:
        """Register a new observer into the system.

        :param observer: The observer to be registered.
        :return: This method does not return anything.
        """
        self._observers.add(observer)

    def unregister_observer(self, observer: TransactionalMixin) -> None:
        """Unregister an observer form the system

        :param observer: The observer to be unregistered
        :return: This method does not return anything.
        """
        self._observers.remove(observer)

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

        :param uuid: Identifier of the ``TransactionEntry``.
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
        uuid_in: Optional[Iterable[UUID]] = None,
        destination_uuid: Optional[UUID] = None,
        status: Optional[TransactionStatus] = None,
        status_in: Optional[Iterable[str]] = None,
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
