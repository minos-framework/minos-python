"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from typing import (
    NoReturn,
)

import aiopg
from psycopg2 import (
    ProgrammingError,
)

from .abc import (
    MinosRepository,
)
from .entries import (
    MinosRepositoryEntry,
)


class PostgreSqlMinosRepository(MinosRepository):
    """TODO"""

    def __init__(self, host: str, port: int, database: str, user: str, password: str):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password

    async def _generate_next_id(self) -> int:
        await self._create_events_table()

        response = await self._submit_sql(_SELECT_NEXT_ID_QUERY)
        next_id = response[0][0]
        return next_id

    async def _generate_next_aggregate_id(self, aggregate_name: str) -> int:
        await self._create_events_table()

        response = await self._submit_sql(_SELECT_NEXT_AGGREGATE_ID_QUERY, (aggregate_name,))

        next_aggregate_id = response[0][0]
        return next_aggregate_id

    async def _get_next_version_id(self, aggregate_name: str, aggregate_id: int) -> int:
        await self._create_events_table()

        response = await self._submit_sql(_SELECT_NEXT_VERSION_QUERY, (aggregate_name, aggregate_id,),)
        next_version_id = response[0][0]
        return next_version_id

    async def _submit(self, entry: MinosRepositoryEntry) -> NoReturn:
        await self._create_events_table()

        params = (entry.id, entry.action.value, entry.aggregate_id, entry.aggregate_name, entry.version, entry.data)
        await self._submit_sql(_INSERT_VALUES_QUERY, params)

    async def select(self, aggregate_id, aggregate_name, *args, **kwargs) -> list[MinosRepositoryEntry]:
        """TODO
        :param aggregate_id: TODO
        :param aggregate_name: TODO
        :param args: TODO
        :param kwargs: TODO
        :return: TODO
        """
        await self._create_events_table()

        params = (aggregate_id, aggregate_name)
        response = await self._submit_sql(_SELECT_ENTRIES_QUERY, params)

        entries = [
            MinosRepositoryEntry(aggregate_id, aggregate_name, row[2], row[3], row[0], row[1]) for row in response
        ]
        return entries

    async def _create_events_table(self):
        await self._submit_sql(_CREATE_TABLE_QUERY, fetch=False)

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


_CREATE_TABLE_QUERY = """
CREATE TABLE IF NOT EXISTS events (
    id integer,
    action text,
    aggregate_id integer,
    aggregate_name text,
    version integer,
    data bytea
);
""".strip()

_SELECT_NEXT_ID_QUERY = """
SELECT CASE COUNT(*) WHEN 0 THEN 1 ELSE MAX(id) + 1 END
FROM events;
""".strip()

_SELECT_NEXT_AGGREGATE_ID_QUERY = """
SELECT CASE COUNT(*) WHEN 0 THEN 1 ELSE MAX(aggregate_id) + 1 END
FROM events
WHERE aggregate_name = %s;
""".strip()

_SELECT_NEXT_VERSION_QUERY = """
SELECT CASE COUNT(*) WHEN 0 THEN 1 ELSE MAX(version) + 1 END
FROM events
WHERE aggregate_name = %s AND aggregate_id = %s;
""".strip()

_INSERT_VALUES_QUERY = """
INSERT INTO events (id, action, aggregate_id, aggregate_name, version, data) VALUES (%s, %s, %s, %s, %s, %s)
""".strip()

_SELECT_ENTRIES_QUERY = """
SELECT id, action, version, data
FROM events
WHERE aggregate_id = %s AND aggregate_name = %s;
""".strip()
