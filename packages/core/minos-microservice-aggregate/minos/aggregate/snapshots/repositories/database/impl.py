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
    DatabaseMixin,
    Inject,
    NotProvidedException,
    ProgrammingException,
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
from .factories import (
    SnapshotDatabaseOperationFactory,
)

if TYPE_CHECKING:
    from ....entities import (
        RootEntity,
    )


class DatabaseSnapshotRepository(SnapshotRepository, DatabaseMixin[SnapshotDatabaseOperationFactory]):
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
        database_key: Optional[tuple[str]] = None,
        **kwargs,
    ):
        if database_key is None:
            database_key = ("aggregate", "snapshot")
        super().__init__(*args, database_key=database_key, **kwargs)

        if event_repository is None:
            raise NotProvidedException("An event repository instance is required.")

        if transaction_repository is None:
            raise NotProvidedException("A transaction repository instance is required.")

        self._event_repository = event_repository
        self._transaction_repository = transaction_repository

    async def _setup(self) -> None:
        operation = self.database_operation_factory.build_create()
        await self.execute_on_database(operation)

    async def _destroy(self) -> None:
        await super()._destroy()

    # noinspection PyUnusedLocal
    async def _find_entries(
        self,
        name: str,
        condition: _Condition,
        ordering: Optional[_Ordering],
        limit: Optional[int],
        streaming_mode: bool,
        transaction: Optional[TransactionEntry],
        exclude_deleted: bool,
        **kwargs,
    ) -> AsyncIterator[SnapshotEntry]:
        if transaction is None:
            transaction_uuids = (NULL_UUID,)
        else:
            transaction_uuids = await transaction.uuids

        operation = self.database_operation_factory.build_query(
            name, condition, ordering, limit, transaction_uuids, exclude_deleted
        )

        async for row in self.execute_on_database_and_fetch_all(operation, streaming_mode=streaming_mode):
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
        operation = self.database_operation_factory.build_query_offset()
        # noinspection PyBroadException
        try:
            row = await self.execute_on_database_and_fetch_one(operation)
        except ProgrammingException:
            return 0
        return row[0]

    async def _store_offset(self, offset: int) -> None:
        operation = self.database_operation_factory.build_submit_offset(offset)
        await self.execute_on_database(operation)

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
        snapshot_entry = await self.get_entry(name, uuid, **kwargs)
        return snapshot_entry.build(**kwargs)

    async def _submit_entry(self, snapshot_entry: SnapshotEntry) -> SnapshotEntry:
        operation = self.database_operation_factory.build_submit(**snapshot_entry.as_raw())
        response = await self.execute_on_database_and_fetch_one(operation)

        snapshot_entry.created_at, snapshot_entry.updated_at = response

        return snapshot_entry

    async def _clean_transactions(self, offset: int, **kwargs) -> None:
        iterable = self._transaction_repository.select(
            event_offset_gt=offset, status_in=(TransactionStatus.COMMITTED, TransactionStatus.REJECTED), **kwargs
        )
        transaction_uuids = {transaction.uuid async for transaction in iterable}
        if len(transaction_uuids):
            operation = self.database_operation_factory.build_delete(transaction_uuids)
            await self.execute_on_database(operation)
