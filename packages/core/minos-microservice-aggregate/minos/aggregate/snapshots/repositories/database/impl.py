from __future__ import (
    annotations,
)

from collections.abc import (
    AsyncIterator,
)
from contextlib import (
    suppress,
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
from minos.transactions import (
    TransactionEntry,
    TransactionNotFoundException,
    TransactionRepository,
    TransactionStatus,
)

from ....deltas import (
    Delta,
    DeltaEntry,
    DeltaRepository,
)
from ....exceptions import (
    NotFoundException,
    SnapshotRepositoryConflictException,
)
from ....queries import (
    _Condition,
    _Ordering,
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
        Entity,
    )


class DatabaseSnapshotRepository(SnapshotRepository, DatabaseMixin[SnapshotDatabaseOperationFactory]):
    """Database Snapshot Repository class.

    The snapshot provides a direct accessor to the ``Entity`` instances stored as deltas by the delta repository
    class.
    """

    @Inject()
    def __init__(
        self,
        *args,
        delta_repository: DeltaRepository,
        transaction_repository: TransactionRepository,
        database_key: Optional[tuple[str]] = None,
        **kwargs,
    ):
        if database_key is None:
            database_key = ("aggregate", "snapshot")
        super().__init__(*args, database_key=database_key, **kwargs)

        if delta_repository is None:
            raise NotProvidedException("An delta repository instance is required.")

        if transaction_repository is None:
            raise NotProvidedException("A transaction repository instance is required.")

        self._delta_repository = delta_repository
        self._transaction_repository = transaction_repository

    async def _setup(self) -> None:
        await super()._setup()
        operation = self.database_operation_factory.build_create()
        await self.execute_on_database(operation)

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
        """Check if the snapshot has the latest version of a ``Entity`` instance.

        :param name: Class name of the ``Entity`` to be checked.
        :return: ``True`` if it has the latest version for the identifier or ``False`` otherwise.
        """
        offset = await self._load_offset()
        iterable = self._delta_repository.select(id_gt=offset, name=name, **kwargs)
        try:
            await iterable.__anext__()
            return False
        except StopAsyncIteration:
            return True

    async def _synchronize(self, **kwargs) -> None:
        initial_offset = await self._load_offset()

        transaction_uuids = set()
        offset = initial_offset
        async for delta_entry in self._delta_repository.select(id_gt=offset, **kwargs):
            with suppress(SnapshotRepositoryConflictException):
                await self._dispatch_one(delta_entry, **kwargs)

            offset = max(delta_entry.id, offset)
            transaction_uuids.add(delta_entry.transaction_uuid)

        await self._clean_transactions(transaction_uuids)

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

    async def _dispatch_one(self, delta_entry: DeltaEntry, **kwargs) -> SnapshotEntry:
        if delta_entry.action.is_delete:
            return await self._submit_delete(delta_entry)

        return await self._submit_update_or_create(delta_entry, **kwargs)

    async def _submit_delete(self, delta_entry: DeltaEntry) -> SnapshotEntry:
        snapshot_entry = SnapshotEntry.from_delta_entry(delta_entry)
        snapshot_entry = await self._submit_entry(snapshot_entry)
        return snapshot_entry

    async def _submit_update_or_create(self, delta_entry: DeltaEntry, **kwargs) -> SnapshotEntry:
        instance = await self._build_instance(delta_entry, **kwargs)

        snapshot_entry = SnapshotEntry.from_entity(instance, transaction_uuid=delta_entry.transaction_uuid)
        snapshot_entry = await self._submit_entry(snapshot_entry)
        return snapshot_entry

    async def _build_instance(self, delta_entry: DeltaEntry, **kwargs) -> Entity:
        diff = delta_entry.delta

        try:
            transaction = await self._transaction_repository.get(uuid=delta_entry.transaction_uuid)
        except TransactionNotFoundException:
            transaction = None

        instance = await self._update_instance_if_exists(diff, transaction=transaction, **kwargs)
        return instance

    async def _update_instance_if_exists(self, delta: Delta, **kwargs) -> Entity:
        # noinspection PyBroadException
        try:
            # noinspection PyTypeChecker
            previous = await self._select_one_instance(delta.name, delta.uuid, **kwargs)
        except NotFoundException:
            # noinspection PyTypeChecker
            cls = import_module(delta.name)
            return cls.from_diff(delta, **kwargs)

        if previous.version >= delta.version:
            raise SnapshotRepositoryConflictException(previous, delta)

        previous.apply_diff(delta)
        return previous

    async def _select_one_instance(self, name: str, uuid: UUID, **kwargs) -> Entity:
        snapshot_entry = await self.get_entry(name, uuid, **kwargs)
        return snapshot_entry.build(**kwargs)

    async def _submit_entry(self, snapshot_entry: SnapshotEntry) -> SnapshotEntry:
        operation = self.database_operation_factory.build_submit(**snapshot_entry.as_raw())
        response = await self.execute_on_database_and_fetch_one(operation)

        snapshot_entry.created_at, snapshot_entry.updated_at = response

        return snapshot_entry

    async def _clean_transactions(self, uuids: set[UUID], **kwargs) -> None:
        if not len(uuids):
            return

        iterable = self._transaction_repository.select(
            uuid_in=uuids,
            status_in=(TransactionStatus.COMMITTED, TransactionStatus.REJECTED),
            **kwargs,
        )
        uuids = {transaction.uuid async for transaction in iterable}

        if not len(uuids):
            return

        operation = self.database_operation_factory.build_delete(uuids)
        await self.execute_on_database(operation)
