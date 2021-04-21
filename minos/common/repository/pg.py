"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""

import aiopg
from psycopg2 import ProgrammingError

from .abc import MinosRepository
from .entries import MinosRepositoryEntry


class PostgreSqlMinosRepository(MinosRepository):
    """TODO"""

    def __init__(self, host: str, port: int, database: str, user: str, password: str):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password

    async def setup(self):
        """TODO

        :return: TODO
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

    async def select(
        self, aggregate_id: int = None, aggregate_name: str = None, *args, **kwargs,
    ) -> list[MinosRepositoryEntry]:
        """TODO
        :param aggregate_id: TODO
        :param aggregate_name: TODO
        :param args: TODO
        :param kwargs: TODO
        :return: TODO
        """
        if aggregate_id is None and aggregate_name is None:
            response = await self._submit_sql(_SELECT_ALL_ENTRIES_QUERY)
            entries = [MinosRepositoryEntry(*row) for row in response]
        else:
            params = (aggregate_id, aggregate_name)
            response = await self._submit_sql(_SELECT_ENTRIES_QUERY, params)
            entries = [MinosRepositoryEntry(aggregate_id, aggregate_name, *row) for row in response]

        return entries

    async def _submit_sql(self, query: str, *args, fetch: bool = True, **kwargs):
        async with self._connection() as connect:
            async with connect.cursor() as cursor:
                await cursor.execute(query, *args, **kwargs)
                if not fetch:
                    return None
                try:
                    return await cursor.fetchall()
                except ProgrammingError:
                    return None

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
