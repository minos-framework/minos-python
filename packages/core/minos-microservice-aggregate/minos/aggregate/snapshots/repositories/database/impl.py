from __future__ import (
    annotations,
)

from collections.abc import (
    AsyncIterator,
)
from typing import (
    TYPE_CHECKING,
    Optional,
)
from uuid import (
    UUID,
)

from minos.common import (
    NULL_UUID,
    Config,
    Inject,
    NotProvidedException,
    import_module,
)

from ....events import (
    Event,
    EventEntry,
    EventRepository,
)
from ....exceptions import (
    NotFoundException,
    SnapshotRepositoryConflictException,
    TransactionNotFoundException,
)
from ....queries import (
    _Condition,
    _EqualCondition,
    _Ordering,
)
from ....transactions import (
    TransactionEntry,
    TransactionRepository,
    TransactionStatus,
)
from ...entries import (
    SnapshotEntry,
)
from ..abc import (
    SnapshotRepository,
)

if TYPE_CHECKING:
    from ....entities import (
        RootEntity,
    )


class DatabaseSnapshotRepository(SnapshotRepository):
    """Database Snapshot Repository class.

    The snapshot provides a direct accessor to the ``RootEntity`` instances stored as events by the event repository
    class.
    """

    @Inject()
    def __init__(
        self,
        *args,
        event_repository: EventRepository,
        transaction_repository: TransactionRepository,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)

        if event_repository is None:
            raise NotProvidedException("An event repository instance is required.")

        if transaction_repository is None:
            raise NotProvidedException("A transaction repository instance is required.")

        self._event_repository = event_repository
        self._transaction_repository = transaction_repository

    @classmethod
    def _from_config(cls, config: Config, **kwargs) -> DatabaseSnapshotRepository:
        return cls(database_key=None, **kwargs)

    async def _setup(self) -> None:
        operation = self.operation_factory.build_create_table()
        await self.submit_query(operation)

    async def _destroy(self) -> None:
        await super()._destroy()

    async def _get(self, name: str, uuid: UUID, **kwargs) -> RootEntity:
        snapshot_entry = await self.get_entry(name, uuid, **kwargs)
        instance = snapshot_entry.build(**kwargs)
        return instance

        # noinspection PyUnusedLocal

    async def get_entry(self, name: str, uuid: UUID, **kwargs) -> SnapshotEntry:
        """Get a ``SnapshotEntry`` from its identifier.

        :param name: Class name of the ``RootEntity``.
        :param uuid: Identifier of the ``RootEntity``.
        :param kwargs: Additional named arguments.
        :return: The ``SnapshotEntry`` instance.
        """

        try:
            return await self.find_entries(
                name, _EqualCondition("uuid", uuid), **kwargs | {"exclude_deleted": False}
            ).__anext__()
        except StopAsyncIteration:
            raise NotFoundException(f"The instance could not be found: {uuid!s}")

    async def _find(self, *args, **kwargs) -> AsyncIterator[RootEntity]:
        async for snapshot_entry in self.find_entries(*args, **kwargs):
            yield snapshot_entry.build(**kwargs)

        # noinspection PyUnusedLocal

    async def find_entries(
        self,
        name: str,
        condition: _Condition,
        ordering: Optional[_Ordering] = None,
        limit: Optional[int] = None,
        streaming_mode: bool = False,
        transaction: Optional[TransactionEntry] = None,
        exclude_deleted: bool = True,
        **kwargs,
    ) -> AsyncIterator[SnapshotEntry]:
        """Find a collection of ``SnapshotEntry`` instances based on a ``Condition``.

        :param name: Class name of the ``RootEntity``.
        :param condition: The condition that must be satisfied by the ``RootEntity`` instances.
        :param ordering: Optional argument to return the instance with specific ordering strategy. The default behaviour
            is to retrieve them without any order pattern.
        :param limit: Optional argument to return only a subset of instances. The default behaviour is to return all the
            instances that meet the given condition.
        :param streaming_mode: If ``True`` return the values in streaming directly from the database (keep an open
            database connection), otherwise preloads the full set of values on memory and then retrieves them.
        :param transaction: The transaction within the operation is performed. If not any value is provided, then the
            transaction is extracted from the context var. If not any transaction is being scoped then the query is
            performed to the global snapshot.
        :param exclude_deleted: If ``True``, deleted ``RootEntity`` entries are included, otherwise deleted
            ``RootEntity`` entries are filtered.
        :param kwargs: Additional named arguments.
        :return: An asynchronous iterator that containing the ``RootEntity`` instances.
        """
        if transaction is None:
            transaction_uuids = (NULL_UUID,)
        else:
            transaction_uuids = await transaction.uuids

        operation = self.operation_factory.build_query(
            name, condition, ordering, limit, transaction_uuids, exclude_deleted
        )

        async for row in self.submit_query_and_iter(operation, streaming_mode=streaming_mode):
            yield SnapshotEntry(*row)

    async def is_synced(self, name: str, **kwargs) -> bool:
        """Check if the snapshot has the latest version of a ``RootEntity`` instance.

        :param name: Class name of the ``RootEntity`` to be checked.
        :return: ``True`` if it has the latest version for the identifier or ``False`` otherwise.
        """
        offset = await self._load_offset()
        iterable = self._event_repository.select(id_gt=offset, name=name, **kwargs)
        try:
            await iterable.__anext__()
            return False
        except StopAsyncIteration:
            return True

    async def _synchronize(self, **kwargs) -> None:
        initial_offset = await self._load_offset()

        offset = initial_offset
        async for event_entry in self._event_repository.select(id_gt=offset, **kwargs):
            try:
                await self._dispatch_one(event_entry, **kwargs)
            except SnapshotRepositoryConflictException:
                pass
            offset = max(event_entry.id, offset)

        if initial_offset < offset:
            await self._clean_transactions(initial_offset)

        await self._store_offset(offset)

    async def _load_offset(self) -> int:
        operation = self.operation_factory.build_get_offset()
        # noinspection PyBroadException
        try:
            raw = await self.submit_query_and_fetchone(operation)
            return raw[0]
        except Exception:
            return 0

    async def _store_offset(self, offset: int) -> None:
        operation = self.operation_factory.build_store_offset(offset)
        await self.submit_query(operation)

    async def _dispatch_one(self, event_entry: EventEntry, **kwargs) -> SnapshotEntry:
        if event_entry.action.is_delete:
            return await self._submit_delete(event_entry)

        return await self._submit_update_or_create(event_entry, **kwargs)

    async def _submit_delete(self, event_entry: EventEntry) -> SnapshotEntry:
        snapshot_entry = SnapshotEntry.from_event_entry(event_entry)
        snapshot_entry = await self._submit_entry(snapshot_entry)
        return snapshot_entry

    async def _submit_update_or_create(self, event_entry: EventEntry, **kwargs) -> SnapshotEntry:
        instance = await self._build_instance(event_entry, **kwargs)

        snapshot_entry = SnapshotEntry.from_root_entity(instance, transaction_uuid=event_entry.transaction_uuid)
        snapshot_entry = await self._submit_entry(snapshot_entry)
        return snapshot_entry

    async def _build_instance(self, event_entry: EventEntry, **kwargs) -> RootEntity:
        diff = event_entry.event

        try:
            transaction = await self._transaction_repository.get(uuid=event_entry.transaction_uuid)
        except TransactionNotFoundException:
            transaction = None

        instance = await self._update_instance_if_exists(diff, transaction=transaction, **kwargs)
        return instance

    async def _update_instance_if_exists(self, event: Event, **kwargs) -> RootEntity:
        # noinspection PyBroadException
        try:
            # noinspection PyTypeChecker
            previous = await self._select_one_instance(event.name, event.uuid, **kwargs)
        except NotFoundException:
            # noinspection PyTypeChecker
            cls = import_module(event.name)
            return cls.from_diff(event, **kwargs)

        if previous.version >= event.version:
            raise SnapshotRepositoryConflictException(previous, event)

        previous.apply_diff(event)
        return previous

    async def _select_one_instance(self, name: str, uuid: UUID, **kwargs) -> RootEntity:
        snapshot_entry = await self._reader.get_entry(name, uuid, **kwargs)
        return snapshot_entry.build(**kwargs)

    async def _submit_entry(self, snapshot_entry: SnapshotEntry) -> SnapshotEntry:
        operation = self.operation_factory.build_insert(**snapshot_entry.as_raw())
        response = await self.submit_query_and_fetchone(operation)

        snapshot_entry.created_at, snapshot_entry.updated_at = response

        return snapshot_entry

    async def _clean_transactions(self, offset: int, **kwargs) -> None:
        iterable = self._transaction_repository.select(
            event_offset_gt=offset, status_in=(TransactionStatus.COMMITTED, TransactionStatus.REJECTED), **kwargs
        )
        transaction_uuids = {transaction.uuid async for transaction in iterable}
        if len(transaction_uuids):
            operation = self.operation_factory.build_delete_by_transactions(transaction_uuids)
            await self.submit_query(operation)
