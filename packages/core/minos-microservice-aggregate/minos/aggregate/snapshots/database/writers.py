from __future__ import (
    annotations,
)

from typing import (
    TYPE_CHECKING,
    Type,
)
from uuid import (
    UUID,
)

from minos.common import (
    Inject,
    NotProvidedException,
    import_module,
)

from ...events import (
    Event,
    EventEntry,
    EventRepository,
)
from ...exceptions import (
    NotFoundException,
    SnapshotRepositoryConflictException,
    TransactionNotFoundException,
)
from ...transactions import (
    TransactionRepository,
    TransactionStatus,
)
from ..entries import (
    SnapshotEntry,
)
from .abc import (
    DatabaseSnapshotSetup,
)
from .readers import (
    DatabaseSnapshotReader,
)

if TYPE_CHECKING:
    from ...entities import (
        RootEntity,
    )


class DatabaseSnapshotWriter(DatabaseSnapshotSetup):
    """Minos Snapshot Dispatcher class."""

    @Inject()
    def __init__(
        self,
        *args,
        reader: DatabaseSnapshotReader,
        event_repository: EventRepository,
        transaction_repository: TransactionRepository,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)

        if event_repository is None:
            raise NotProvidedException("An event repository instance is required.")

        if transaction_repository is None:
            raise NotProvidedException("A transaction repository instance is required.")

        self._reader = reader
        self._event_repository = event_repository
        self._transaction_repository = transaction_repository

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

    async def dispatch(self, **kwargs) -> None:
        """Perform a dispatching step, based on the sequence of non already processed ``EventEntry`` objects.

        :return: This method does not return anything.
        """
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
            cls: Type[RootEntity] = import_module(event.name)
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