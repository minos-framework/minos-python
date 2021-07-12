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
    RepositoryAction,
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

    async def are_synced(self, aggregate_name: str, aggregate_uuids: list[UUID]) -> bool:
        """Check if the snapshot has the latest version of a list of aggregates.

        :param aggregate_name: Class name of the ``Aggregate`` to be checked.
        :param aggregate_uuids: List of aggregate identifiers to be checked.

        :return: ``True`` if has the latest version for all the identifiers or ``False`` otherwise.
        """
        for aggregate_uuid in aggregate_uuids:
            if not await self.is_synced(aggregate_name, aggregate_uuid):
                return False
        return True

    async def is_synced(self, aggregate_name: str, aggregate_uuid: UUID) -> bool:
        """Check if the snapshot has the latest version of an ``Aggregate`` instance.

        :param aggregate_name: Class name of the ``Aggregate`` to be checked.
        :param aggregate_uuid: Identifier of the ``Aggregate`` instance to be checked.
        :return: ``True`` if has the latest version for the identifier or ``False`` otherwise.
        """
        query = self._repository.select(
            id_ge=await self._load_offset(), aggregate_name=aggregate_name, aggregate_uuid=aggregate_uuid
        )
        async for _ in query:
            return False
        return True

    async def dispatch(self) -> NoReturn:
        """Perform a dispatching step, based on the sequence of non already processed ``RepositoryEntry`` objects.

        :return: This method does not return anything.
        """
        offset = await self._load_offset()

        ids = set()

        def _update_offset(e: RepositoryEntry, o: int):
            ids.add(e.id)
            while o + 1 in ids:
                o += 1
                ids.remove(o)
            return o

        async for entry in self._repository.select(id_gt=offset):
            try:
                await self._dispatch_one(entry)
            except MinosPreviousVersionSnapshotException:
                pass
            offset = _update_offset(entry, offset)

        await self._store_offset(offset)

    async def _load_offset(self) -> int:
        # noinspection PyBroadException
        try:
            raw = await self.submit_query_and_fetchone(_SELECT_OFFSET_QUERY)
            return raw[0]
        except Exception:
            return 0

    async def _store_offset(self, offset: int) -> NoReturn:
        await self.submit_query(_INSERT_OFFSET_QUERY, {"value": offset})

    async def _dispatch_one(self, event_entry: RepositoryEntry) -> Optional[SnapshotEntry]:
        if event_entry.action is RepositoryAction.DELETE:
            return await self._submit_delete(event_entry)

        instance = await self._build_instance(event_entry)
        return await self._submit_instance(instance)

    async def _submit_delete(self, entry: RepositoryEntry) -> NoReturn:
        params = {
            "aggregate_uuid": entry.aggregate_uuid,
            "aggregate_name": entry.aggregate_name,
            "version": entry.version,
            "data": None,
        }
        await self.submit_query_and_fetchone(_INSERT_ONE_SNAPSHOT_ENTRY_QUERY, params)

    async def _build_instance(self, event_entry: RepositoryEntry) -> Aggregate:
        # noinspection PyTypeChecker
        diff = event_entry.aggregate_diff
        instance = await self._update_if_exists(diff)
        return instance

    async def _update_if_exists(self, diff: AggregateDiff) -> Aggregate:
        # noinspection PyBroadException
        try:
            # noinspection PyTypeChecker
            previous = await self._select_one_aggregate(diff.uuid, diff.name)
        except Exception:
            # noinspection PyTypeChecker
            aggregate_cls: Type[Aggregate] = import_module(diff.name)
            return aggregate_cls.from_diff(diff)

        if previous.version >= diff.version:
            raise MinosPreviousVersionSnapshotException(previous, diff)

        previous.apply_diff(diff)
        return previous

    async def _select_one_aggregate(self, aggregate_uuid: UUID, aggregate_name: str) -> Aggregate:
        snapshot_entry = await self._select_one(aggregate_uuid, aggregate_name)
        return snapshot_entry.aggregate

    async def _select_one(self, aggregate_uuid: UUID, aggregate_name: str) -> SnapshotEntry:
        raw = await self.submit_query_and_fetchone(_SELECT_ONE_SNAPSHOT_ENTRY_QUERY, (aggregate_uuid, aggregate_name))
        return SnapshotEntry(aggregate_uuid, aggregate_name, *raw)

    async def _submit_instance(self, aggregate: Aggregate) -> SnapshotEntry:
        snapshot_entry = SnapshotEntry.from_aggregate(aggregate)
        snapshot_entry = await self._submit_update_or_create(snapshot_entry)
        return snapshot_entry

    async def _submit_update_or_create(self, entry: SnapshotEntry) -> SnapshotEntry:
        params = {
            "aggregate_uuid": entry.aggregate_uuid,
            "aggregate_name": entry.aggregate_name,
            "version": entry.version,
            "data": entry.data,
        }
        response = await self.submit_query_and_fetchone(_INSERT_ONE_SNAPSHOT_ENTRY_QUERY, params)

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
    default,
    default
)
ON CONFLICT (aggregate_uuid, aggregate_name)
DO
   UPDATE SET version = %(version)s, data = %(data)s, updated_at = NOW()
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
