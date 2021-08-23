"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from __future__ import (
    annotations,
)

from typing import (
    TYPE_CHECKING,
    NoReturn,
    Optional,
    Type,
)
from uuid import (
    UUID,
)

from dependency_injector.wiring import (
    Provide,
)

from ...configuration import (
    MinosConfig,
)
from ...exceptions import (
    MinosPreviousVersionSnapshotException,
    MinosRepositoryNotProvidedException,
)
from ...importlib import (
    import_module,
)
from ...repository import (
    MinosRepository,
    RepositoryEntry,
)
from ..entries import (
    SnapshotEntry,
)
from .abc import (
    PostgreSqlSnapshotSetup,
)

if TYPE_CHECKING:
    from ...model import (
        Aggregate,
        AggregateDiff,
    )


class PostgreSqlSnapshotBuilder(PostgreSqlSnapshotSetup):
    """Minos Snapshot Dispatcher class."""

    _repository: MinosRepository = Provide["repository"]

    def __init__(self, *args, repository: Optional[MinosRepository] = None, **kwargs):
        super().__init__(*args, **kwargs)
        if repository is not None:
            self._repository = repository

        if self._repository is None or isinstance(self._repository, Provide):
            raise MinosRepositoryNotProvidedException("A repository instance is required.")

    @classmethod
    def _from_config(cls, *args, config: MinosConfig, **kwargs) -> PostgreSqlSnapshotBuilder:
        return cls(*args, **config.snapshot._asdict(), **kwargs)

    async def are_synced(self, aggregate_name: str, aggregate_uuids: set[UUID], **kwargs) -> bool:
        """Check if the snapshot has the latest version of a list of aggregates.

        :param aggregate_name: Class name of the ``Aggregate`` to be checked.
        :param aggregate_uuids: List of aggregate identifiers to be checked.

        :return: ``True`` if has the latest version for all the identifiers or ``False`` otherwise.
        """
        for aggregate_uuid in aggregate_uuids:
            if not await self.is_synced(aggregate_name, aggregate_uuid, **kwargs):
                return False
        return True

    async def is_synced(self, aggregate_name: str, aggregate_uuid: UUID, **kwargs) -> bool:
        """Check if the snapshot has the latest version of an ``Aggregate`` instance.

        :param aggregate_name: Class name of the ``Aggregate`` to be checked.
        :param aggregate_uuid: Identifier of the ``Aggregate`` instance to be checked.
        :return: ``True`` if has the latest version for the identifier or ``False`` otherwise.
        """
        offset = await self._load_offset(**kwargs)
        iterable = self._repository.select(
            id_ge=offset, aggregate_name=aggregate_name, aggregate_uuid=aggregate_uuid, **kwargs
        )
        try:
            await iterable.__anext__()
            return False
        except StopAsyncIteration:
            return True

    async def dispatch(self, **kwargs) -> NoReturn:
        """Perform a dispatching step, based on the sequence of non already processed ``RepositoryEntry`` objects.

        :return: This method does not return anything.
        """
        offset = await self._load_offset(**kwargs)

        ids = set()

        def _update_offset(e: RepositoryEntry, o: int):
            ids.add(e.id)
            while o + 1 in ids:
                o += 1
                ids.remove(o)
            return o

        async for entry in self._repository.select(id_gt=offset, **kwargs):
            try:
                await self._dispatch_one(entry, **kwargs)
            except MinosPreviousVersionSnapshotException:
                pass
            offset = _update_offset(entry, offset)

        await self._store_offset(offset)

    async def _load_offset(self, **kwargs) -> int:
        # noinspection PyBroadException
        try:
            raw = await self.submit_query_and_fetchone(_SELECT_OFFSET_QUERY, **kwargs)
            return raw[0]
        except Exception:
            return 0

    async def _store_offset(self, offset: int) -> NoReturn:
        await self.submit_query(_INSERT_OFFSET_QUERY, {"value": offset})

    async def _dispatch_one(self, event_entry: RepositoryEntry, **kwargs) -> Optional[SnapshotEntry]:
        if event_entry.action.is_delete:
            return await self._submit_delete(event_entry, **kwargs)

        instance = await self._build_instance(event_entry, **kwargs)
        return await self._submit_instance(instance, **kwargs)

    async def _submit_delete(self, entry: RepositoryEntry, **kwargs) -> NoReturn:
        params = {
            "aggregate_uuid": entry.aggregate_uuid,
            "aggregate_name": entry.aggregate_name,
            "version": entry.version,
            "data": None,
            "created_at": entry.created_at,
            "updated_at": entry.created_at,
        }
        await self.submit_query_and_fetchone(_INSERT_ONE_SNAPSHOT_ENTRY_QUERY, params, **kwargs)

    async def _build_instance(self, event_entry: RepositoryEntry, **kwargs) -> Aggregate:
        # noinspection PyTypeChecker
        diff = event_entry.aggregate_diff
        instance = await self._update_if_exists(diff, **kwargs)
        return instance

    async def _update_if_exists(self, aggregate_diff: AggregateDiff, **kwargs) -> Aggregate:
        # noinspection PyBroadException
        try:
            # noinspection PyTypeChecker
            previous = await self._select_one_aggregate(aggregate_diff.uuid, aggregate_diff.name, **kwargs)
        except Exception:
            # noinspection PyTypeChecker
            aggregate_cls: Type[Aggregate] = import_module(aggregate_diff.name)
            return aggregate_cls.from_diff(aggregate_diff, **kwargs)

        if previous.version >= aggregate_diff.version:
            raise MinosPreviousVersionSnapshotException(previous, aggregate_diff)

        previous.apply_diff(aggregate_diff)
        return previous

    async def _select_one_aggregate(self, aggregate_uuid: UUID, aggregate_name: str, **kwargs) -> Aggregate:
        snapshot_entry = await self._select_one(aggregate_uuid, aggregate_name, **kwargs)
        return snapshot_entry.build_aggregate(**kwargs)

    async def _select_one(self, aggregate_uuid: UUID, aggregate_name: str, **kwargs) -> SnapshotEntry:
        raw = await self.submit_query_and_fetchone(
            _SELECT_ONE_SNAPSHOT_ENTRY_QUERY, (aggregate_uuid, aggregate_name), **kwargs
        )
        return SnapshotEntry(aggregate_uuid, aggregate_name, *raw)

    async def _submit_instance(self, aggregate: Aggregate, **kwargs) -> SnapshotEntry:
        snapshot_entry = SnapshotEntry.from_aggregate(aggregate)
        snapshot_entry = await self._submit_update_or_create(snapshot_entry, **kwargs)
        return snapshot_entry

    async def _submit_update_or_create(self, entry: SnapshotEntry, **kwargs) -> SnapshotEntry:
        params = {
            "aggregate_uuid": entry.aggregate_uuid,
            "aggregate_name": entry.aggregate_name,
            "version": entry.version,
            "data": entry.data,
            "created_at": entry.created_at,
            "updated_at": entry.updated_at,
        }
        response = await self.submit_query_and_fetchone(_INSERT_ONE_SNAPSHOT_ENTRY_QUERY, params, **kwargs)

        entry.created_at, entry.updated_at = response

        return entry


_SELECT_ONE_SNAPSHOT_ENTRY_QUERY = """
SELECT version, data, created_at, updated_at
FROM snapshot
WHERE aggregate_uuid = %s and aggregate_name = %s;
""".strip()

_INSERT_ONE_SNAPSHOT_ENTRY_QUERY = """
INSERT INTO snapshot (aggregate_uuid, aggregate_name, version, data, created_at, updated_at)
VALUES (
    %(aggregate_uuid)s,
    %(aggregate_name)s,
    %(version)s,
    %(data)s,
    %(created_at)s,
    %(updated_at)s
)
ON CONFLICT (aggregate_uuid, aggregate_name)
DO
   UPDATE SET version = %(version)s, data = %(data)s, updated_at = %(updated_at)s
RETURNING created_at, updated_at;
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
DO UPDATE SET value = %(value)s;
""".strip()
