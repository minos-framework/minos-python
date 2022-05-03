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

from minos.common import (
    NULL_UUID,
    Inject,
    Injectable,
    Lock,
    LockPool,
    NotProvidedException,
    PoolFactory,
    SetupMixin,
    classname,
)
from minos.networks import (
    BrokerMessageV1,
    BrokerMessageV1Payload,
    BrokerMessageV1Strategy,
    BrokerPublisher,
)

from ...actions import (
    Action,
)
from ...contextvars import (
    IS_REPOSITORY_SERIALIZATION_CONTEXT_VAR,
)
from ...exceptions import (
    EventRepositoryConflictException,
    EventRepositoryException,
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
from ..fields import (
    IncrementalFieldDiff,
)
from ..models import (
    Event,
)

if TYPE_CHECKING:
    from ...entities import (
        RootEntity,
    )


@Injectable("event_repository")
class EventRepository(ABC, SetupMixin):
    """Base event repository class in ``minos``."""

    @Inject()
    def __init__(
        self,
        broker_publisher: BrokerPublisher,
        transaction_repository: TransactionRepository,
        lock_pool: Optional[LockPool] = None,
        pool_factory: Optional[PoolFactory] = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)

        if lock_pool is None and pool_factory is not None:
            lock_pool = pool_factory.get_pool("lock")

        if broker_publisher is None:
            raise NotProvidedException("A broker instance is required.")

        if transaction_repository is None:
            raise NotProvidedException("A transaction repository instance is required.")

        if lock_pool is None:
            raise NotProvidedException("A lock pool instance is required.")

        self._broker_publisher = broker_publisher
        self._transaction_repository = transaction_repository
        self._lock_pool = lock_pool

    def transaction(self, **kwargs) -> TransactionEntry:
        """Build a transaction instance related to the repository.

        :param kwargs: Additional named arguments.
        :return: A new ``TransactionEntry`` instance.
        """
        return TransactionEntry(event_repository=self, transaction_repository=self._transaction_repository, **kwargs)

    async def create(self, entry: Union[Event, EventEntry]) -> EventEntry:
        """Store new creation entry into the repository.

        :param entry: Entry to be stored.
        :return: The repository entry containing the stored information.
        """

        entry.action = Action.CREATE
        return await self.submit(entry)

    async def update(self, entry: Union[Event, EventEntry]) -> EventEntry:
        """Store new update entry into the repository.

        :param entry: Entry to be stored.
        :return: The repository entry containing the stored information.
        """

        entry.action = Action.UPDATE
        return await self.submit(entry)

    async def delete(self, entry: Union[Event, EventEntry]) -> EventEntry:
        """Store new deletion entry into the repository.

        :param entry: Entry to be stored.
        :return: The repository entry containing the stored information.
        """

        entry.action = Action.DELETE
        return await self.submit(entry)

    async def submit(self, entry: Union[Event, EventEntry], **kwargs) -> EventEntry:
        """Store new entry into the repository.

        :param entry: The entry to be stored.
        :param kwargs: Additional named arguments.
        :return: The repository entry containing the stored information.
        """

        token = IS_REPOSITORY_SERIALIZATION_CONTEXT_VAR.set(True)
        try:
            transaction = TRANSACTION_CONTEXT_VAR.get()

            if isinstance(entry, Event):
                entry = EventEntry.from_event(entry, transaction=transaction)

            if not isinstance(entry.action, Action):
                raise EventRepositoryException("The 'EventEntry.action' attribute must be an 'Action' instance.")

            async with self.write_lock():
                if not await self.validate(entry, **kwargs):
                    raise EventRepositoryConflictException(f"{entry!r} could not be committed!", await self.offset)

                entry = await self._submit(entry, **kwargs)

            if entry.transaction_uuid == NULL_UUID:
                await self._send_events(entry.event)

        finally:
            IS_REPOSITORY_SERIALIZATION_CONTEXT_VAR.reset(token)

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
    async def _submit(self, entry: EventEntry, **kwargs) -> EventEntry:
        raise NotImplementedError

    async def _send_events(self, event: Event):
        suffix_mapper = {
            Action.CREATE: "Created",
            Action.UPDATE: "Updated",
            Action.DELETE: "Deleted",
        }
        topic = f"{event.simplified_name}{suffix_mapper[event.action]}"
        message = BrokerMessageV1(
            topic=topic,
            payload=BrokerMessageV1Payload(content=event),
            strategy=BrokerMessageV1Strategy.MULTICAST,
        )
        futures = [self._broker_publisher.send(message)]

        if event.action == Action.UPDATE:
            for decomposed_event in event.decompose():
                diff = next(iter(decomposed_event.fields_diff.flatten_values()))
                composed_topic = f"{topic}.{diff.name}"
                if isinstance(diff, IncrementalFieldDiff):
                    composed_topic += f".{diff.action.value}"

                message = BrokerMessageV1(
                    topic=composed_topic,
                    payload=BrokerMessageV1Payload(content=decomposed_event),
                    strategy=BrokerMessageV1Strategy.MULTICAST,
                )
                futures.append(self._broker_publisher.send(message))

        await gather(*futures)

    # noinspection PyShadowingBuiltins
    async def select(
        self,
        uuid: Optional[UUID] = None,
        name: Optional[Union[str, type[RootEntity]]] = None,
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
