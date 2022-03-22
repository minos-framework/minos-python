from __future__ import (
    annotations,
)

from typing import (
    AsyncIterator,
    Optional,
)
from uuid import (
    UUID,
)

from psycopg2 import (
    IntegrityError,
)
from psycopg2.sql import (
    SQL,
    Composable,
    Literal,
    Placeholder,
)

from minos.common import (
    NULL_UUID,
    Config,
    PostgreSqlMinosDatabase,
)

from ...exceptions import (
    EventRepositoryConflictException,
)
from ..entries import (
    EventEntry,
)
from .abc import (
    EventRepository,
)


class PostgreSqlEventRepository(PostgreSqlMinosDatabase, EventRepository):
    """PostgreSQL-based implementation of the event repository class in ``Minos``."""

    @classmethod
    def _from_config(cls, *args, config: Config, **kwargs) -> Optional[EventRepository]:
        return cls(*args, **config.get_database_by_name("event"), **kwargs)

    async def _setup(self):
        """Setup miscellaneous repository thing.

        In the PostgreSQL case, configures the needed table to be used to store the data.

        :return: This method does not return anything.
        """
        await self.submit_query('CREATE EXTENSION IF NOT EXISTS "uuid-ossp";', lock="uuid-ossp")

        await self.submit_query(_CREATE_ACTION_ENUM_QUERY, lock="aggregate_event")
        await self.submit_query(_CREATE_TABLE_QUERY, lock="aggregate_event")

    async def _submit(self, entry: EventEntry, **kwargs) -> EventEntry:
        lock = None
        if entry.uuid != NULL_UUID:
            lock = entry.uuid.int & (1 << 32) - 1

        query, params = await self._build_query(entry)

        try:
            response = await self.submit_query_and_fetchone(query, params, lock=lock)
        except IntegrityError:
            raise EventRepositoryConflictException(
                f"{entry!r} could not be submitted due to a key (uuid, version, transaction) collision",
                await self.offset,
            )

        entry.id, entry.uuid, entry.version, entry.created_at = response
        return entry

    async def _build_query(self, entry: EventEntry) -> tuple[Composable, dict[str, UUID]]:
        if entry.transaction_uuid != NULL_UUID:
            transaction = await self._transaction_repository.get(uuid=entry.transaction_uuid)
            transaction_uuids = await transaction.uuids
        else:
            transaction_uuids = (NULL_UUID,)

        from_query_parts = list()
        parameters = dict()
        for index, transaction_uuid in enumerate(transaction_uuids, start=1):
            name = f"transaction_uuid_{index}"
            parameters[name] = transaction_uuid

            from_query_parts.append(
                _SELECT_TRANSACTION_CHUNK.format(index=Literal(index), transaction_uuid=Placeholder(name))
            )

        from_query = SQL(" UNION ALL ").join(from_query_parts)

        query = _INSERT_VALUES_QUERY.format(from_parts=from_query)

        return query, parameters | entry.as_raw()

    async def _select(self, **kwargs) -> AsyncIterator[EventEntry]:
        query = self._build_select_query(**kwargs)
        async for row in self.submit_query_and_iter(query, kwargs, **kwargs):
            yield EventEntry(*row)

    # noinspection PyUnusedLocal
    @staticmethod
    def _build_select_query(
        uuid: Optional[UUID] = None,
        name: Optional[str] = None,
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
        transaction_uuid: Optional[UUID] = None,
        transaction_uuid_ne: Optional[UUID] = None,
        transaction_uuid_in: Optional[tuple[UUID, ...]] = None,
        **kwargs,
    ) -> str:
        conditions = list()

        if uuid is not None:
            conditions.append("uuid = %(uuid)s")
        if name is not None:
            conditions.append("name = %(name)s")
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
        if transaction_uuid is not None:
            conditions.append("transaction_uuid = %(transaction_uuid)s")
        if transaction_uuid_ne is not None:
            conditions.append("transaction_uuid <> %(transaction_uuid_ne)s")
        if transaction_uuid_in is not None:
            conditions.append("transaction_uuid IN %(transaction_uuid_in)s")

        if not conditions:
            return f"{_SELECT_ALL_ENTRIES_QUERY} ORDER BY id;"

        return f"{_SELECT_ALL_ENTRIES_QUERY} WHERE {' AND '.join(conditions)} ORDER BY id;"

    @property
    async def _offset(self) -> int:
        return (await self.submit_query_and_fetchone(_SELECT_MAX_ID_QUERY))[0] or 0


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
    uuid UUID NOT NULL,
    name TEXT NOT NULL,
    version INT NOT NULL,
    data BYTEA NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    transaction_uuid UUID NOT NULL DEFAULT uuid_nil(),
    UNIQUE (uuid, version, transaction_uuid)
);
""".strip()

_INSERT_VALUES_QUERY = SQL(
    """
INSERT INTO aggregate_event (id, action, uuid, name, version, data, created_at, transaction_uuid)
VALUES (
    default,
    %(action)s,
    CASE %(uuid)s WHEN uuid_nil() THEN uuid_generate_v4() ELSE %(uuid)s END,
    %(name)s,
    (
        SELECT (CASE WHEN %(version)s IS NULL THEN 1 + COALESCE(MAX(t2.version), 0) ELSE %(version)s END)
        FROM (
                 SELECT DISTINCT ON (t1.uuid) t1.version
                 FROM ( {from_parts} ) AS t1
                 ORDER BY t1.uuid, t1.transaction_index DESC
        ) AS t2
    ),
    %(data)s,
    (CASE WHEN %(created_at)s IS NULL THEN NOW() ELSE %(created_at)s END),
    %(transaction_uuid)s
)
RETURNING id, uuid, version, created_at;
    """
)

_SELECT_TRANSACTION_CHUNK = SQL(
    """
SELECT {index} AS transaction_index, uuid, MAX(version) AS version
FROM aggregate_event
WHERE uuid = %(uuid)s AND transaction_uuid = {transaction_uuid}
GROUP BY uuid
    """
)

_SELECT_ALL_ENTRIES_QUERY = """
SELECT uuid, name, version, data, id, action, created_at, transaction_uuid
FROM aggregate_event
""".strip()

_SELECT_MAX_ID_QUERY = "SELECT MAX(id) FROM aggregate_event;".strip()
