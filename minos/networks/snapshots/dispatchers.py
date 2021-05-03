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

import aiopg
from minos.common import (
    Aggregate,
    MinosConfig,
    MinosConfigException,
    MinosRepositoryAction,
    MinosRepositoryEntry,
    MinosSetup,
    PostgreSqlMinosRepository,
    import_module,
)

from ..exceptions import (
    MinosPreviousVersionSnapshotException,
)
from .entries import (
    MinosSnapshotEntry,
)


class MinosSnapshotDispatcher(MinosSetup):
    """Minos Snapshot Dispatcher class."""

    def __init__(
        self,
        *args,
        host: str = None,
        port: int = None,
        database: str = None,
        user: str = None,
        password: str = None,
        repository: dict[str, Any] = None,
        offset: int = 0,
        **kwargs
    ):
        super().__init__(*args, **kwargs)

        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password

        self.offset = offset

        self.repository = PostgreSqlMinosRepository(**repository)

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
        return cls(*args, **config.repository._asdict(), repository=config.repository._asdict(), **kwargs)

    async def _setup(self) -> NoReturn:
        await self._create_broker_table()

    async def _create_broker_table(self) -> NoReturn:
        await self._submit_sql(_CREATE_TABLE_QUERY)

    # noinspection PyUnusedLocal
    async def select(self, *args, **kwargs) -> AsyncIterator[MinosSnapshotEntry]:
        """Select a sequence of ``MinosSnapshotEntry`` objects.

        :param args: Additional positional arguments.
        :param kwargs: Additional named arguments.
        :return: A sequence of ``MinosSnapshotEntry`` objects.
        """
        async for row in self._submit_and_iter_sql(_SELECT_ALL_ENTRIES_QUERY):
            yield MinosSnapshotEntry(*row)

    async def dispatch(self) -> NoReturn:
        """Perform a dispatching step, based on the sequence of non already processed ``MinosRepositoryEntry`` objects.

        :return: This method does not return anything.
        """
        async for entry in self._new_entries:
            try:
                await self._dispatch_one(entry)
            except MinosPreviousVersionSnapshotException:
                continue

    @property
    async def _new_entries(self):
        for entry in await self.repository.select(id_ge=self.offset):
            yield entry

    async def _dispatch_one(self, event_entry: MinosRepositoryEntry) -> Optional[MinosSnapshotEntry]:
        if event_entry.action is MinosRepositoryAction.DELETE:
            await self._submit_delete(event_entry)
            return
        instance = await self._build_instance(event_entry)
        return await self._submit_instance(instance)

    async def _submit_delete(self, entry: MinosRepositoryEntry) -> NoReturn:
        params = {"aggregate_id": entry.aggregate_id, "aggregate_name": entry.aggregate_name}
        await self._submit_sql(_DELETE_ONE_SNAPSHOT_ENTRY_QUERY, params)

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
        raw = await self._submit_and_fetchone_sql(_SELECT_ONE_SNAPSHOT_ENTRY_QUERY, (aggregate_id, aggregate_name))
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
        response = await self._submit_and_fetchone_sql(_INSERT_ONE_SNAPSHOT_ENTRY_QUERY, params)

        entry.created_at, entry.updated_at = response

        return entry

    async def _submit_and_fetchone_sql(self, *args, **kwargs) -> tuple:
        return await self._submit_and_iter_sql(*args, **kwargs).__anext__()

    async def _submit_and_iter_sql(self, query: str, *args, **kwargs) -> AsyncIterator[tuple]:
        async with self._connection() as connect:
            async with connect.cursor() as cursor:
                await cursor.execute(query, *args, **kwargs)
                async for row in cursor:
                    yield row

    async def _submit_sql(self, query: str, *args, **kwargs) -> NoReturn:
        async with self._connection() as connect:
            async with connect.cursor() as cursor:
                await cursor.execute(query, *args, **kwargs)

    def _connection(self):
        return aiopg.connect(
            host=self.host, port=self.port, dbname=self.database, user=self.user, password=self.password
        )


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
