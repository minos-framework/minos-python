"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from __future__ import (
    annotations,
)

from typing import (
    Any,
    AsyncIterator,
    NoReturn,
    Optional,
    Type,
)

from minos.common import (
    Aggregate,
    MinosConfig,
    MinosConfigException,
    MinosRepositoryAction,
    MinosRepositoryEntry,
    PostgreSqlMinosDatabase,
    PostgreSqlMinosRepository,
    import_module,
)

from ..exceptions import (
    MinosPreviousVersionSnapshotException,
)
from .entries import (
    MinosSnapshotEntry,
)


class MinosSnapshotDispatcher(PostgreSqlMinosDatabase):
    """Minos Snapshot Dispatcher class."""

    def __init__(self, *args, repository: dict[str, Any] = None, **kwargs):
        super().__init__(*args, **kwargs)
        self.repository = PostgreSqlMinosRepository(**repository)

    async def _destroy(self) -> NoReturn:
        await super()._destroy()
        await self.repository.destroy()

    @classmethod
    def from_config(cls, *args, config: MinosConfig = None, **kwargs) -> MinosSnapshotDispatcher:
        """Build a new Snapshot Dispatcher from config.
        :param args: Additional positional arguments.
        :param config: Config instance. If `None` is provided, default config is chosen.
        :param kwargs: Additional named arguments.
        :return: A `MinosRepository` instance.
        """
        if config is None:
            config = MinosConfig.get_default()
        if config is None:
            raise MinosConfigException("The config object must be setup.")
        # noinspection PyProtectedMember
        return cls(*args, **config.snapshot._asdict(), repository=config.repository._asdict(), **kwargs)

    async def _setup(self) -> NoReturn:
        await self.submit_query(_CREATE_TABLE_QUERY)
        await self.submit_query(_CREATE_OFFSET_TABLE_QUERY)

    async def dispatch(self) -> NoReturn:
        """Perform a dispatching step, based on the sequence of non already processed ``MinosRepositoryEntry`` objects.

        :return: This method does not return anything.
        """
        offset = await self._load_offset()

        ids = set()

        def _update_offset(e: MinosRepositoryEntry, o: int):
            ids.add(e.id)
            while o + 1 in ids:
                o += 1
                ids.remove(o)
            return o

        async for entry in self.repository.select(id_ge=offset):
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

    # noinspection PyUnusedLocal
    async def select(self, *args, **kwargs) -> AsyncIterator[MinosSnapshotEntry]:
        """Select a sequence of ``MinosSnapshotEntry`` objects.

        :param args: Additional positional arguments.
        :param kwargs: Additional named arguments.
        :return: A sequence of ``MinosSnapshotEntry`` objects.
        """
        async for row in self.submit_query_and_iter(_SELECT_ALL_ENTRIES_QUERY):
            yield MinosSnapshotEntry(*row)

    async def _dispatch_one(self, event_entry: MinosRepositoryEntry) -> Optional[MinosSnapshotEntry]:
        if event_entry.action is MinosRepositoryAction.DELETE:
            await self._submit_delete(event_entry)
            return
        instance = await self._build_instance(event_entry)
        return await self._submit_instance(instance)

    async def _submit_delete(self, entry: MinosRepositoryEntry) -> NoReturn:
        params = {"aggregate_id": entry.aggregate_id, "aggregate_name": entry.aggregate_name}
        await self.submit_query(_DELETE_ONE_SNAPSHOT_ENTRY_QUERY, params)

    async def _build_instance(self, event_entry: MinosRepositoryEntry) -> Aggregate:
        # noinspection PyTypeChecker
        cls: Type[Aggregate] = import_module(event_entry.aggregate_name)
        instance = cls.from_avro_bytes(event_entry.data, id=event_entry.aggregate_id, version=event_entry.version)
        instance = await self._update_if_exists(instance)
        return instance

    async def _update_if_exists(self, new: Aggregate) -> Aggregate:
        # noinspection PyBroadException
        try:
            # noinspection PyTypeChecker
            previous = await self._select_one_aggregate(new.id, new.classname)
        except Exception:
            return new

        if previous.version >= new.version:
            raise MinosPreviousVersionSnapshotException(previous, new)

        new._fields = previous.fields | new.fields
        return new

    async def _select_one_aggregate(self, aggregate_id: int, aggregate_name: str) -> Aggregate:
        snapshot_entry = await self._select_one(aggregate_id, aggregate_name)
        return snapshot_entry.aggregate

    async def _select_one(self, aggregate_id: int, aggregate_name: str) -> MinosSnapshotEntry:
        raw = await self.submit_query_and_fetchone(_SELECT_ONE_SNAPSHOT_ENTRY_QUERY, (aggregate_id, aggregate_name))
        return MinosSnapshotEntry(aggregate_id, aggregate_name, *raw)

    async def _submit_instance(self, aggregate: Aggregate) -> MinosSnapshotEntry:
        snapshot_entry = MinosSnapshotEntry.from_aggregate(aggregate)
        snapshot_entry = await self._submit_update_or_create(snapshot_entry)
        return snapshot_entry

    async def _submit_update_or_create(self, entry: MinosSnapshotEntry) -> MinosSnapshotEntry:
        params = {
            "aggregate_id": entry.aggregate_id,
            "aggregate_name": entry.aggregate_name,
            "version": entry.version,
            "data": entry.data,
        }
        response = await self.submit_query_and_fetchone(_INSERT_ONE_SNAPSHOT_ENTRY_QUERY, params)

        entry.created_at, entry.updated_at = response

        return entry


_CREATE_TABLE_QUERY = """
CREATE TABLE IF NOT EXISTS snapshot (
    aggregate_id BIGINT NOT NULL,
    aggregate_name TEXT NOT NULL,
    version INT NOT NULL,
    data BYTEA NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (aggregate_id, aggregate_name)
);
""".strip()

_SELECT_ALL_ENTRIES_QUERY = """
SELECT aggregate_id, aggregate_name, version, data, created_at, updated_at
FROM snapshot;
""".strip()

_DELETE_ONE_SNAPSHOT_ENTRY_QUERY = """
DELETE FROM snapshot
WHERE aggregate_id = %(aggregate_id)s and aggregate_name = %(aggregate_name)s;
""".strip()

_SELECT_ONE_SNAPSHOT_ENTRY_QUERY = """
SELECT version, data, created_at, updated_at
FROM snapshot
WHERE aggregate_id = %s and aggregate_name = %s;
""".strip()

_INSERT_ONE_SNAPSHOT_ENTRY_QUERY = """
INSERT INTO snapshot (aggregate_id, aggregate_name, version, data, created_at, updated_at)
VALUES (
    %(aggregate_id)s,
    %(aggregate_name)s,
    %(version)s,
    %(data)s,
    default,
    default
)
ON CONFLICT (aggregate_id, aggregate_name)
DO
   UPDATE SET version = %(version)s, data = %(data)s, updated_at = NOW()
RETURNING created_at, updated_at;
""".strip()

_CREATE_OFFSET_TABLE_QUERY = """
CREATE TABLE IF NOT EXISTS snapshot_aux_offset (
    id bool PRIMARY KEY DEFAULT TRUE,
    value BIGINT NOT NULL,
    CONSTRAINT id_uni CHECK (id)
);
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
