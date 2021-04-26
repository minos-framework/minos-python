"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from typing import (
    Any,
    Optional,
)

import aiopg

from .abc import (
    MinosRepository,
)
from .entries import (
    MinosRepositoryEntry,
)


class PostgreSqlMinosRepository(MinosRepository):
    """PostgreSQL-based implementation of the repository class in ``minos``."""

    def __init__(
        self,
        host: str = None,
        port: int = None,
        database: str = None,
        user: str = None,
        password: str = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password

    async def _setup(self):
        """Setup miscellaneous repository thing.

        In the PostgreSQL case, configures the needed table to be used to store the data.

        :return: This method does not return anything.
        """
        await self._create_events_table()

    async def _create_events_table(self):
        await self._submit_sql(_CREATE_ACTION_ENUM_QUERY, fetch=False)
        await self._submit_sql(_CREATE_TABLE_QUERY, fetch=False)

    async def _submit(self, entry: MinosRepositoryEntry) -> MinosRepositoryEntry:
        params = {
            "action": entry.action.value,
            "aggregate_id": entry.aggregate_id,
            "aggregate_name": entry.aggregate_name,
            "data": entry.data,
        }
        response = await self._submit_sql(_INSERT_VALUES_QUERY, params)
        entry.id = response[0][0]
        entry.aggregate_id = response[0][1]
        entry.version = response[0][2]
        return entry

    async def _select(
        self, aggregate_id: int = None, aggregate_name: str = None, *args, **kwargs,
    ) -> list[MinosRepositoryEntry]:
        if aggregate_id is None and aggregate_name is None:
            response = await self._submit_sql(_SELECT_ALL_ENTRIES_QUERY)
            entries = [MinosRepositoryEntry(*row) for row in response]
        else:
            params = (aggregate_id, aggregate_name)
            response = await self._submit_sql(_SELECT_ENTRIES_QUERY, params)
            entries = [MinosRepositoryEntry(aggregate_id, aggregate_name, *row) for row in response]

        return entries

    async def _submit_sql(self, query: str, *args, fetch: bool = True, **kwargs) -> Optional[list[tuple[Any, ...]]]:
        async with self._connection() as connect:
            async with connect.cursor() as cursor:
                await cursor.execute(query, *args, **kwargs)
                if not fetch:
                    return None
                return await cursor.fetchall()

    def _connection(self):
        return aiopg.connect(
            host=self.host, port=self.port, dbname=self.database, user=self.user, password=self.password,
        )


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
            CREATE TYPE action_type AS ENUM ('insert', 'update', 'delete');
        END IF;
    END;
$$
LANGUAGE plpgsql;
""".strip()

_CREATE_TABLE_QUERY = """
CREATE TABLE IF NOT EXISTS events (
    id BIGSERIAL PRIMARY KEY,
    action ACTION_TYPE NOT NULL,
    aggregate_id BIGINT NOT NULL,
    aggregate_name TEXT NOT NULL,
    version INT NOT NULL,
    data BYTEA NOT NULL,
    UNIQUE (aggregate_id, aggregate_name, version)
);
""".strip()

_INSERT_VALUES_QUERY = """
INSERT INTO events (id, action, aggregate_id, aggregate_name, version, data)
VALUES (default,
        %(action)s,
        (
            CASE %(aggregate_id)s
                WHEN 0 THEN (
                    SELECT (CASE COUNT(*) WHEN 0 THEN 1 ELSE MAX(aggregate_id) + 1 END)
                    FROM events
                    WHERE aggregate_name = %(aggregate_name)s
                )
                ELSE %(aggregate_id)s END
            ),
        %(aggregate_name)s,
        (
            SELECT (CASE COUNT(*) WHEN 0 THEN 1 ELSE MAX(version) + 1 END)
            FROM events
            WHERE aggregate_id = %(aggregate_id)s
              AND aggregate_name = %(aggregate_name)s
        ),
        %(data)s)
RETURNING id, aggregate_id, version;
""".strip()

_SELECT_ENTRIES_QUERY = """
SELECT version, data, id, action
FROM events
WHERE aggregate_id = %s AND aggregate_name = %s;
""".strip()

_SELECT_ALL_ENTRIES_QUERY = """
SELECT aggregate_id, aggregate_name, version, data, id, action
FROM events
""".strip()
