from __future__ import (
    annotations,
)

from abc import (
    ABC,
    abstractmethod,
)
from asyncio import (
    gather,
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

from dependency_injector.wiring import (
    Provide,
    inject,
)

from ...exceptions import (
    MinosBrokerNotProvidedException,
    MinosLockPoolNotProvidedException,
    MinosRepositoryConflictException,
    MinosRepositoryException,
    MinosTransactionRepositoryNotProvidedException,
)
from ...locks import (
    Lock,
)
from ...networks import (
    MinosBroker,
)
from ...pools import (
    MinosPool,
)
from ...setup import (
    MinosSetup,
)
from ...transactions import (
    TRANSACTION_CONTEXT_VAR,
    TransactionEntry,
    TransactionRepository,
    TransactionStatus,
)
from ..entries import (
    EventEntry,
)

if TYPE_CHECKING:
    from ...model import (
        AggregateDiff,
    )


class EventRepository(ABC, MinosSetup):
    """Base event repository class in ``minos``."""

    @inject
    def __init__(
        self,
        event_broker: MinosBroker = Provide["event_broker"],
        transaction_repository: TransactionRepository = Provide["transaction_repository"],
        lock_pool: MinosPool[Lock] = Provide["lock_pool"],
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)

        if event_broker is None or isinstance(event_broker, Provide):
            raise MinosBrokerNotProvidedException("A broker instance is required.")

        if transaction_repository is None or isinstance(transaction_repository, Provide):
            raise MinosTransactionRepositoryNotProvidedException("A transaction repository instance is required.")

        if lock_pool is None or isinstance(lock_pool, Provide):
            raise MinosLockPoolNotProvidedException("A lock pool instance is required.")

        self._event_broker = event_broker
        self._transaction_repository = transaction_repository
        self._lock_pool = lock_pool

    def transaction(self, **kwargs) -> TransactionEntry:
        """Build a transaction instance related to the repository.

        :param kwargs: Additional named arguments.
        :return: A new ``TransactionEntry`` instance.
        """
        return TransactionEntry(event_repository=self, transaction_repository=self._transaction_repository, **kwargs)

    async def create(self, entry: Union[AggregateDiff, EventEntry]) -> EventEntry:
        """Store new creation entry into the repository.

        :param entry: Entry to be stored.
        :return: The repository entry containing the stored information.
        """
        from ...model import (
            Action,
        )

        entry.action = Action.CREATE
        return await self.submit(entry)

    async def update(self, entry: Union[AggregateDiff, EventEntry]) -> EventEntry:
        """Store new update entry into the repository.

        :param entry: Entry to be stored.
        :return: The repository entry containing the stored information.
        """
        from ...model import (
            Action,
        )

        entry.action = Action.UPDATE
        return await self.submit(entry)

    async def delete(self, entry: Union[AggregateDiff, EventEntry]) -> EventEntry:
        """Store new deletion entry into the repository.

        :param entry: Entry to be stored.
        :return: The repository entry containing the stored information.
        """
        from ...model import (
            Action,
        )

        entry.action = Action.DELETE
        return await self.submit(entry)

    async def submit(self, entry: Union[AggregateDiff, EventEntry], **kwargs) -> EventEntry:
        """Store new entry into the repository.

        :param entry: The entry to be stored.
        :param kwargs: Additional named arguments.
        :return: The repository entry containing the stored information.
        """
        from ...model import (
            Action,
            AggregateDiff,
        )

        transaction = TRANSACTION_CONTEXT_VAR.get()

        if isinstance(entry, AggregateDiff):
            entry = EventEntry.from_aggregate_diff(entry, transaction=transaction)

        if not isinstance(entry.action, Action):
            raise MinosRepositoryException("The 'EventEntry.action' attribute must be an 'Action' instance.")

        async with self.write_lock():
            if not await self.validate(entry, **kwargs):
                raise MinosRepositoryConflictException(f"{entry!r} could not be committed!", await self.offset)

            entry = await self._submit(entry, **kwargs)

        if transaction is None:
            await self._send_events(entry.aggregate_diff)

        return entry

    # noinspection PyUnusedLocal
    async def validate(self, entry: EventEntry, transaction_uuid_ne: Optional[UUID] = None, **kwargs) -> bool:
        """Check if it is able to submit the given entry.

        :param entry: The entry to be validated.
        :param transaction_uuid_ne: Optional transaction identifier to skip it from the validation.
        :param kwargs: Additional named arguments.
        :return: ``True`` if the entry can be submitted or ``False`` otherwise.
        """
        iterable = self._transaction_repository.select(
            destination=entry.transaction_uuid,
            uuid_ne=transaction_uuid_ne,
            status_in=(TransactionStatus.RESERVING, TransactionStatus.RESERVED, TransactionStatus.COMMITTING,),
        )

        transaction_uuids = {e.uuid async for e in iterable}

        if len(transaction_uuids):
            with suppress(StopAsyncIteration):
                iterable = self.select(
                    aggregate_uuid=entry.aggregate_uuid, transaction_uuid_in=tuple(transaction_uuids), **kwargs
                )

                await iterable.__anext__()  # Will raise a `StopAsyncIteration` exception if not any item.

                return False

        return True

    @abstractmethod
    async def _submit(self, entry: EventEntry, **kwargs) -> EventEntry:
        raise NotImplementedError

    async def _send_events(self, aggregate_diff: AggregateDiff):
        from ...model import (
            Action,
        )

        suffix_mapper = {
            Action.CREATE: "Created",
            Action.UPDATE: "Updated",
            Action.DELETE: "Deleted",
        }

        topic = f"{aggregate_diff.simplified_name}{suffix_mapper[aggregate_diff.action]}"
        futures = [self._event_broker.send(aggregate_diff, topic=topic)]

        if aggregate_diff.action == Action.UPDATE:
            from ...model import (
                IncrementalFieldDiff,
            )

            for decomposed_aggregate_diff in aggregate_diff.decompose():
                diff = next(iter(decomposed_aggregate_diff.fields_diff.flatten_values()))
                composed_topic = f"{topic}.{diff.name}"
                if isinstance(diff, IncrementalFieldDiff):
                    composed_topic += f".{diff.action.value}"
                futures.append(self._event_broker.send(decomposed_aggregate_diff, topic=composed_topic))

        await gather(*futures)

    # noinspection PyShadowingBuiltins
    async def select(
        self,
        aggregate_uuid: Optional[UUID] = None,
        aggregate_name: Optional[str] = None,
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
    ) -> AsyncIterator[EventEntry]:
        """Perform a selection query of entries stored in to the repository.

        :param aggregate_uuid: Aggregate identifier.
        :param aggregate_name: Aggregate name.
        :param version: Aggregate version.
        :param version_lt: Aggregate version lower than the given value.
        :param version_gt: Aggregate version greater than the given value.
        :param version_le: Aggregate version lower or equal to the given value.
        :param version_ge: Aggregate version greater or equal to the given value.
        :param id: Entry identifier.
        :param id_lt: Entry identifier lower than the given value.
        :param id_gt: Entry identifier greater than the given value.
        :param id_le: Entry identifier lower or equal to the given value.
        :param id_ge: Entry identifier greater or equal to the given value.
        :param transaction_uuid: Transaction identifier.
        :param transaction_uuid_ne: Transaction identifier distinct of the given value.
        :param transaction_uuid_in: TODO
        :return: A list of entries.
        """
        generator = self._select(
            aggregate_uuid=aggregate_uuid,
            aggregate_name=aggregate_name,
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
    async def _select(self, *args, **kwargs) -> AsyncIterator[EventEntry]:
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
        """Get a write lock.

        :return: An asynchronous context manager.
        """
        return self._lock_pool.acquire("aggregate_event_write_lock")
