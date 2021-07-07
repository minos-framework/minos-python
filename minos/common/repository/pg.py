"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from typing import (
    AsyncIterator,
    Optional,
)

from ..database import (
    PostgreSqlMinosDatabase,
)
from .abc import (
    MinosRepository,
)
from .entries import (
    RepositoryEntry,
)


class PostgreSqlRepository(MinosRepository, PostgreSqlMinosDatabase):
    """PostgreSQL-based implementation of the repository class in ``Minos``."""

    async def _setup(self):
        """Setup miscellaneous repository thing.

        In the PostgreSQL case, configures the needed table to be used to store the data.

        :return: This method does not return anything.
        """
        await self._create_events_table()

    async def _create_events_table(self):
        await self.submit_query(_CREATE_ACTION_ENUM_QUERY, lock=hash("aggregate_event"))
        await self.submit_query(_CREATE_TABLE_QUERY, lock=hash("aggregate_event"))

    async def _submit(self, entry: RepositoryEntry) -> RepositoryEntry:
        params = {
            "action": entry.action.value,
            "aggregate_id": entry.aggregate_id,
            "aggregate_name": entry.aggregate_name,
            "data": entry.data,
        }
        response = await self.submit_query_and_fetchone(_INSERT_VALUES_QUERY, params)
        entry.id, entry.aggregate_id, entry.version, entry.created_at = response
        return entry

    async def _select(self, **kwargs,) -> AsyncIterator[RepositoryEntry]:
        query = self._build_select_query(**kwargs)
        async for row in self.submit_query_and_iter(query, kwargs):
            yield RepositoryEntry(*row)

    # noinspection PyUnusedLocal
    @staticmethod
    def _build_select_query(
        aggregate_id: Optional[int] = None,
        aggregate_name: Optional[str] = None,
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
        **kwargs,
    ) -> str:
        conditions = list()

        if aggregate_id is not None:
            conditions.append("aggregate_id = %(aggregate_id)s")
        if aggregate_name is not None:
            conditions.append("aggregate_name = %(aggregate_name)s")
        if version is not None:
            conditions.append("version = %(version)s")
        if version_lt is not None:
            conditions.append("version < %(version_lt)s")
        if version_gt is not None:
            conditions.append("version > %(version_gt)s")
        if version_le is not None:
            conditions.append("version <= %(version_le)s")
        if version_ge is not None:
            conditions.append("version >= %(version_ge)s")
        if id is not None:
            conditions.append("id = %(id)s")
        if id_lt is not None:
            conditions.append("id < %(id_lt)s")
        if id_gt is not None:
            conditions.append("id > %(id_gt)s")
        if id_le is not None:
            conditions.append("id <= %(id_le)s")
        if id_ge is not None:
            conditions.append("id >= %(id_ge)s")

        if not conditions:
            return _SELECT_ALL_ENTRIES_QUERY

        return f"{_SELECT_ALL_ENTRIES_QUERY} WHERE {' AND '.join(conditions)}"


_CREATE_ACTION_ENUM_QUERY = """
DO
$$
    BEGIN
        IF NOT EXISTS(SELECT *
                      FROM pg_type typ
                               INNER JOIN pg_namespace nsp
                                          ON nsp.oid = typ.typnamespace
                      WHERE nsp.nspname = current_schema()
                        AND typ.typname = 'action_type') THEN
            CREATE TYPE action_type AS ENUM ('create', 'update', 'delete');
        END IF;
    END;
$$
LANGUAGE plpgsql;
""".strip()

_CREATE_TABLE_QUERY = """
CREATE TABLE IF NOT EXISTS aggregate_event (
    id BIGSERIAL PRIMARY KEY,
    action ACTION_TYPE NOT NULL,
    aggregate_id BIGINT NOT NULL,
    aggregate_name TEXT NOT NULL,
    version INT NOT NULL,
    data BYTEA NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (aggregate_id, aggregate_name, version)
);
""".strip()

_INSERT_VALUES_QUERY = """
INSERT INTO aggregate_event (id, action, aggregate_id, aggregate_name, version, data, created_at)
VALUES (
    default,
    %(action)s,
    (
        CASE %(aggregate_id)s
            WHEN 0 THEN (
                SELECT (CASE COUNT(*) WHEN 0 THEN 1 ELSE MAX(aggregate_id) + 1 END)
                FROM aggregate_event
                WHERE aggregate_name = %(aggregate_name)s
            )
            ELSE %(aggregate_id)s END
        ),
    %(aggregate_name)s,
    (
        SELECT (CASE COUNT(*) WHEN 0 THEN 1 ELSE MAX(version) + 1 END)
        FROM aggregate_event
        WHERE aggregate_id = %(aggregate_id)s
          AND aggregate_name = %(aggregate_name)s
    ),
    %(data)s,
    default
)
RETURNING id, aggregate_id, version, created_at;
""".strip()

_SELECT_ALL_ENTRIES_QUERY = """
SELECT aggregate_id, aggregate_name, version, data, id, action, created_at
FROM aggregate_event
""".strip()
