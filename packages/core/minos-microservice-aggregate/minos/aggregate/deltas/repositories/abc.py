from __future__ import (
    annotations,
)

from abc import (
    ABC,
    abstractmethod,
)
from contextlib import (
    suppress,
)
from typing import (
    TYPE_CHECKING,
    AsyncIterator,
    Awaitable,
    Optional,
    Union,
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
    classname,
)
from minos.transactions import (
    TRANSACTION_CONTEXT_VAR,
    TransactionalMixin,
    TransactionEntry,
    TransactionStatus,
)

from ...actions import (
    Action,
)
from ...contextvars import (
    IS_REPOSITORY_SERIALIZATION_CONTEXT_VAR,
)
from ...exceptions import (
    DeltaRepositoryConflictException,
    DeltaRepositoryException,
)
from ..entries import (
    DeltaEntry,
)
from ..models import (
    Delta,
)

if TYPE_CHECKING:
    from ...entities import (
        Entity,
    )


@Injectable("delta_repository")
class DeltaRepository(ABC, TransactionalMixin):
    """Base delta repository class in ``minos``."""

    _lock_pool: LockPool

    @Inject()
    def __init__(
        self,
        lock_pool: Optional[LockPool] = None,
        pool_factory: Optional[PoolFactory] = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)

        if lock_pool is None and pool_factory is not None:
            lock_pool = pool_factory.get_pool("lock")

        if lock_pool is None:
            raise NotProvidedException("A lock pool instance is required.")

        self._lock_pool = lock_pool

    def transaction(self, **kwargs) -> TransactionEntry:
        """Build a transaction instance related to the repository.

        :param kwargs: Additional named arguments.
        :return: A new ``TransactionEntry`` instance.
        """
        return TransactionEntry(repository=self.transaction_repository, **kwargs)

    async def create(self, entry: Union[Delta, DeltaEntry]) -> DeltaEntry:
        """Store new creation entry into the repository.

        :param entry: Entry to be stored.
        :return: The repository entry containing the stored information.
        """

        entry.action = Action.CREATE
        return await self.submit(entry)

    async def update(self, entry: Union[Delta, DeltaEntry]) -> DeltaEntry:
        """Store new update entry into the repository.

        :param entry: Entry to be stored.
        :return: The repository entry containing the stored information.
        """

        entry.action = Action.UPDATE
        return await self.submit(entry)

    async def delete(self, entry: Union[Delta, DeltaEntry]) -> DeltaEntry:
        """Store new deletion entry into the repository.

        :param entry: Entry to be stored.
        :return: The repository entry containing the stored information.
        """

        entry.action = Action.DELETE
        return await self.submit(entry)

    async def submit(self, entry: Union[Delta, DeltaEntry], **kwargs) -> DeltaEntry:
        """Store new entry into the repository.

        :param entry: The entry to be stored.
        :param kwargs: Additional named arguments.
        :return: The repository entry containing the stored information.
        """

        token = IS_REPOSITORY_SERIALIZATION_CONTEXT_VAR.set(True)
        try:
            transaction = TRANSACTION_CONTEXT_VAR.get()

            if isinstance(entry, Delta):
                entry = DeltaEntry.from_delta(entry, transaction=transaction)

            if not isinstance(entry.action, Action):
                raise DeltaRepositoryException("The 'DeltaEntry.action' attribute must be an 'Action' instance.")

            async with self.write_lock():
                if not await self.validate(entry, **kwargs):
                    raise DeltaRepositoryConflictException(f"{entry!r} could not be committed!", await self.offset)

                entry = await self._submit(entry, **kwargs)

        finally:
            IS_REPOSITORY_SERIALIZATION_CONTEXT_VAR.reset(token)

        return entry

    # noinspection PyUnusedLocal
    async def validate(self, entry: DeltaEntry, transaction_uuid_ne: Optional[UUID] = None, **kwargs) -> bool:
        """Check if it is able to submit the given entry.

        :param entry: The entry to be validated.
        :param transaction_uuid_ne: Optional transaction identifier to skip it from the validation.
        :param kwargs: Additional named arguments.
        :return: ``True`` if the entry can be submitted or ``False`` otherwise.
        """
        iterable = self.transaction_repository.select(
            destination_uuid=entry.transaction_uuid,
            uuid_ne=transaction_uuid_ne,
            status_in=(TransactionStatus.RESERVING, TransactionStatus.RESERVED, TransactionStatus.COMMITTING),
        )

        transaction_uuids = {e.uuid async for e in iterable}

        if len(transaction_uuids):
            with suppress(StopAsyncIteration):
                iterable = self.select(uuid=entry.uuid, transaction_uuid_in=tuple(transaction_uuids), **kwargs)

                await iterable.__anext__()  # Will raise a `StopAsyncIteration` exception if not any item.

                return False

        return True

    @abstractmethod
    async def _submit(self, entry: DeltaEntry, **kwargs) -> DeltaEntry:
        raise NotImplementedError

    # noinspection PyShadowingBuiltins
    async def select(
        self,
        uuid: Optional[UUID] = None,
        name: Optional[Union[str, type[Entity]]] = None,
        version: Optional[int] = None,
        version_lt: Optional[int] = None,
        version_gt: Optional[int] = None,
        version_le: Optional[int] = None,
        version_ge: Optional[int] = None,
        id: Optional[int] = None,
        id_lt: Optional[int] = None,
        id_gt: Optional[int] = None,
        id_le: Optional[int] = None,
        id_ge: Optional[int] = None,
        transaction_uuid: Optional[UUID] = None,
        transaction_uuid_ne: Optional[UUID] = None,
        transaction_uuid_in: Optional[tuple[UUID, ...]] = None,
        **kwargs,
    ) -> AsyncIterator[DeltaEntry]:
        """Perform a selection query of entries stored in to the repository.

        :param uuid: The identifier must be equal to the given value.
        :param name: The classname must be equal to the given value.
        :param version: The version must be equal to the given value.
        :param version_lt: The version must be lower than the given value.
        :param version_gt: The version must be greater than the given value.
        :param version_le: The version must be lower or equal to the given value.
        :param version_ge: The version must be greater or equal to the given value.
        :param id: The entry identifier must be equal to the given value.
        :param id_lt: The entry identifier must be lower than the given value.
        :param id_gt: The entry identifier must be greater than the given value.
        :param id_le: The entry identifier must be lower or equal to the given value.
        :param id_ge: The entry identifier must be greater or equal to the given value.
        :param transaction_uuid: The transaction identifier must be equal to the given value.
        :param transaction_uuid_ne: The transaction identifier must be distinct of the given value.
        :param transaction_uuid_in: The destination transaction identifier must be equal to one of the given values.
        :return: A list of entries.
        """
        if isinstance(name, type):
            name = classname(name)
        generator = self._select(
            uuid=uuid,
            name=name,
            version=version,
            version_lt=version_lt,
            version_gt=version_gt,
            version_le=version_le,
            version_ge=version_ge,
            id=id,
            id_lt=id_lt,
            id_gt=id_gt,
            id_le=id_le,
            id_ge=id_ge,
            transaction_uuid=transaction_uuid,
            transaction_uuid_ne=transaction_uuid_ne,
            transaction_uuid_in=transaction_uuid_in,
            **kwargs,
        )
        # noinspection PyTypeChecker
        async for entry in generator:
            yield entry

    @abstractmethod
    async def _select(self, *args, **kwargs) -> AsyncIterator[DeltaEntry]:
        """Perform a selection query of entries stored in to the repository."""

    @property
    def offset(self) -> Awaitable[int]:
        """Get the current repository offset.

        :return: An awaitable containing an integer value.
        """
        return self._offset

    @property
    @abstractmethod
    async def _offset(self) -> int:
        raise NotImplementedError

    def write_lock(self) -> Lock:
        """Get a lock.

        :return: An asynchronous context manager.
        """
        return self._lock_pool.acquire("aggregate_delta_write_lock")

    async def get_collided_transactions(self, transaction_uuid: UUID) -> set[UUID]:
        """Get the set of collided transaction identifiers.

        :param transaction_uuid: The identifier of the transaction to be committed.
        :return: A ``set`` or ``UUID`` values.
        """
        entries = dict()
        async for entry in self.select(transaction_uuid=transaction_uuid):
            if entry.uuid in entries and entry.version < entries[entry.uuid]:
                continue
            entries[entry.uuid] = entry.version

        transaction_uuids = set()
        for uuid, version in entries.items():
            async for entry in self.select(uuid=uuid, version=version):
                if entry.transaction_uuid != transaction_uuid:
                    transaction_uuids.add(entry.transaction_uuid)
        return transaction_uuids

    async def commit_transaction(self, transaction_uuid: UUID, destination_transaction_uuid: UUID) -> None:
        """Commit the transaction with given identifier.

        :param transaction_uuid: The identifier of the transaction to be committed.
        :param destination_transaction_uuid: The identifier of the destination transaction.
        :return: This method does not return anything.
        """
        async for entry in self.select(transaction_uuid=transaction_uuid):
            new = DeltaEntry.from_another(entry, transaction_uuid=destination_transaction_uuid)
            await self.submit(new, transaction_uuid_ne=transaction_uuid)
