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

from dependency_injector.wiring import (
    Provide,
    inject,
)

from minos.common import (
    NotProvidedException,
    import_module,
)

from ...events import (
    EventEntry,
    EventRepository,
)
from ...exceptions import (
    AggregateNotFoundException,
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
    PostgreSqlSnapshotSetup,
)

if TYPE_CHECKING:
    from ...models import (
        Aggregate,
        AggregateDiff,
    )
    from .readers import (
        PostgreSqlSnapshotReader,
    )


class PostgreSqlSnapshotWriter(PostgreSqlSnapshotSetup):
    """Minos Snapshot Dispatcher class."""

    @inject
    def __init__(
        self,
        *args,
        reader: PostgreSqlSnapshotReader,
        event_repository: EventRepository = Provide["event_repository"],
        transaction_repository: TransactionRepository = Provide["transaction_repository"],
        **kwargs
    ):
        super().__init__(*args, **kwargs)

        if event_repository is None or isinstance(event_repository, Provide):
            raise NotProvidedException("An event repository instance is required.")

        if transaction_repository is None or isinstance(transaction_repository, Provide):
            raise NotProvidedException("A transaction repository instance is required.")

        self._reader = reader
        self._event_repository = event_repository
        self._transaction_repository = transaction_repository

    async def is_synced(self, aggregate_name: str, **kwargs) -> bool:
        """Check if the snapshot has the latest version of an ``Aggregate`` instance.

        :param aggregate_name: Class name of the ``Aggregate`` to be checked.
        :return: ``True`` if it has the latest version for the identifier or ``False`` otherwise.
        """
        offset = await self._load_offset(**kwargs)
        iterable = self._event_repository.select(id_gt=offset, aggregate_name=aggregate_name, **kwargs)
        try:
            await iterable.__anext__()
            return False
        except StopAsyncIteration:
            return True

    async def dispatch(self, **kwargs) -> None:
        """Perform a dispatching step, based on the sequence of non already processed ``EventEntry`` objects.

        :return: This method does not return anything.
        """
        initial_offset = await self._load_offset(**kwargs)

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

    async def _load_offset(self, **kwargs) -> int:
        # noinspection PyBroadException
        try:
            raw = await self.submit_query_and_fetchone(_SELECT_OFFSET_QUERY, **kwargs)
            return raw[0]
        except Exception:
            return 0

    async def _store_offset(self, offset: int) -> None:
        await self.submit_query(_INSERT_OFFSET_QUERY, {"value": offset}, lock="insert_snapshot_aux_offset")

    async def _dispatch_one(self, event_entry: EventEntry, **kwargs) -> SnapshotEntry:
        if event_entry.action.is_delete:
            return await self._submit_delete(event_entry, **kwargs)

        return await self._submit_update_or_create(event_entry, **kwargs)

    async def _submit_delete(self, event_entry: EventEntry, **kwargs) -> SnapshotEntry:
        snapshot_entry = SnapshotEntry.from_event_entry(event_entry)
        snapshot_entry = await self._submit_entry(snapshot_entry, **kwargs)
        return snapshot_entry

    async def _submit_update_or_create(self, event_entry: EventEntry, **kwargs) -> SnapshotEntry:
        aggregate = await self._build_aggregate(event_entry, **kwargs)

        snapshot_entry = SnapshotEntry.from_aggregate(aggregate, transaction_uuid=event_entry.transaction_uuid)
        snapshot_entry = await self._submit_entry(snapshot_entry, **kwargs)
        return snapshot_entry

    async def _build_aggregate(self, event_entry: EventEntry, **kwargs) -> Aggregate:
        diff = event_entry.aggregate_diff

        try:
            transaction = await self._transaction_repository.get(uuid=event_entry.transaction_uuid)
        except TransactionNotFoundException:
            transaction = None

        aggregate = await self._update_if_exists(diff, transaction=transaction, **kwargs)
        return aggregate

    async def _update_if_exists(self, aggregate_diff: AggregateDiff, **kwargs) -> Aggregate:
        # noinspection PyBroadException
        try:
            # noinspection PyTypeChecker
            previous = await self._select_one_aggregate(aggregate_diff.uuid, aggregate_diff.name, **kwargs)
        except AggregateNotFoundException:
            # noinspection PyTypeChecker
            aggregate_cls: Type[Aggregate] = import_module(aggregate_diff.name)
            return aggregate_cls.from_diff(aggregate_diff, **kwargs)

        if previous.version >= aggregate_diff.version:
            raise SnapshotRepositoryConflictException(previous, aggregate_diff)

        previous.apply_diff(aggregate_diff)
        return previous

    async def _select_one_aggregate(self, aggregate_uuid: UUID, aggregate_name: str, **kwargs) -> Aggregate:
        snapshot_entry = await self._reader.get_entry(aggregate_name, aggregate_uuid, **kwargs)
        return snapshot_entry.build_aggregate(**kwargs)

    async def _submit_entry(self, snapshot_entry: SnapshotEntry, **kwargs) -> SnapshotEntry:
        params = snapshot_entry.as_raw()
        response = await self.submit_query_and_fetchone(_INSERT_ONE_SNAPSHOT_ENTRY_QUERY, params, **kwargs)

        snapshot_entry.created_at, snapshot_entry.updated_at = response

        return snapshot_entry

    async def _clean_transactions(self, offset: int, **kwargs) -> None:
        iterable = self._transaction_repository.select(
            event_offset_gt=offset, status_in=(TransactionStatus.COMMITTED, TransactionStatus.REJECTED), **kwargs
        )
        transaction_uuids = {transaction.uuid async for transaction in iterable}
        if len(transaction_uuids):
            await self.submit_query(_DELETE_SNAPSHOT_ENTRIES_QUERY, {"transaction_uuids": tuple(transaction_uuids)})


_INSERT_ONE_SNAPSHOT_ENTRY_QUERY = """
INSERT INTO snapshot (aggregate_uuid, aggregate_name, version, schema, data, created_at, updated_at, transaction_uuid)
VALUES (
    %(aggregate_uuid)s,
    %(aggregate_name)s,
    %(version)s,
    %(schema)s,
    %(data)s,
    %(created_at)s,
    %(updated_at)s,
    %(transaction_uuid)s
)
ON CONFLICT (aggregate_uuid, transaction_uuid)
DO
   UPDATE SET version = %(version)s, schema = %(schema)s, data = %(data)s, updated_at = %(updated_at)s
RETURNING created_at, updated_at;
""".strip()

_DELETE_SNAPSHOT_ENTRIES_QUERY = """
DELETE FROM snapshot
WHERE transaction_uuid IN %(transaction_uuids)s;
""".strip()

_SELECT_OFFSET_QUERY = """
SELECT value
FROM snapshot_aux_offset
WHERE id = TRUE;
"""

_INSERT_OFFSET_QUERY = """
INSERT INTO snapshot_aux_offset (id, value)
VALUES (TRUE, %(value)s)
ON CONFLICT (id)
DO UPDATE SET value = GREATEST(%(value)s, (SELECT value FROM snapshot_aux_offset WHERE id = TRUE));
""".strip()
