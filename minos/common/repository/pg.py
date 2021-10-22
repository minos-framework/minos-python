from typing import (
    AsyncIterator,
    Optional,
)
from uuid import (
    UUID,
)

from ..database import (
    PostgreSqlMinosDatabase,
)
from ..uuid import (
    NULL_UUID,
)
from .abc import (
    MinosRepository,
)
from .entries import (
    RepositoryEntry,
)


class PostgreSqlRepository(PostgreSqlMinosDatabase, MinosRepository):
    """PostgreSQL-based implementation of the repository class in ``Minos``."""

    async def _setup(self):
        """Setup miscellaneous repository thing.

        In the PostgreSQL case, configures the needed table to be used to store the data.

        :return: This method does not return anything.
        """
        await self._create_events_table()

    async def _create_events_table(self):
        await self.submit_query('CREATE EXTENSION IF NOT EXISTS "uuid-ossp";')

        await self.submit_query(_CREATE_TRANSACTION_STATUS_ENUM_QUERY, lock=hash("aggregate_transaction_enum"))
        await self.submit_query(_CREATE_TRANSACTION_TABLE_QUERY, lock=hash("aggregate_transaction"))

        await self.submit_query(_CREATE_ACTION_ENUM_QUERY, lock=hash("aggregate_event"))
        await self.submit_query(_CREATE_TABLE_QUERY, lock=hash("aggregate_event"))

    async def _submit(self, entry: RepositoryEntry) -> RepositoryEntry:
        if entry.transaction_uuid != NULL_UUID:
            # FIXME
            await self.submit_query_and_fetchone(
                _INSERT_TRANSACTIONS_VALUES_QUERY,
                {"uuid": entry.transaction_uuid, "status": "pending"},
                lock=entry.transaction_uuid.int & (1 << 32) - 1,
            )

        params = {
            "action": entry.action.value,
            "aggregate_uuid": entry.aggregate_uuid,
            "aggregate_name": entry.aggregate_name,
            "version": entry.version,
            "data": entry.data,
            "transaction_uuid": entry.transaction_uuid,
            "created_at": entry.created_at,
        }

        lock = None
        if entry.aggregate_uuid != NULL_UUID:
            lock = entry.aggregate_uuid.int & (1 << 32) - 1

        response = await self.submit_query_and_fetchone(_INSERT_VALUES_QUERY, params, lock=lock)
        entry.id, entry.aggregate_uuid, entry.version, entry.created_at = response
        return entry

    async def submit_transaction(self, transaction):
        # FIXME
        await self.submit_query_and_fetchone(
            _INSERT_TRANSACTIONS_VALUES_QUERY,
            {"uuid": transaction.uuid, "status": transaction.status.value},
            lock=transaction.uuid.int & (1 << 32) - 1,
        )

    async def _select(self, **kwargs) -> AsyncIterator[RepositoryEntry]:
        query = self._build_select_query(**kwargs)
        async for row in self.submit_query_and_iter(query, kwargs, **kwargs):
            yield RepositoryEntry(*row)

    # noinspection PyUnusedLocal
    @staticmethod
    def _build_select_query(
        aggregate_uuid: Optional[UUID] = None,
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
        transaction_uuid: Optional[UUID] = None,
        **kwargs,
    ) -> str:
        conditions = list()

        if aggregate_uuid is not None:
            conditions.append("aggregate_uuid = %(aggregate_uuid)s")
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
        if transaction_uuid is not None:
            conditions.append("transaction_uuid = %(transaction_uuid)s")

        if not conditions:
            return f"{_SELECT_ALL_ENTRIES_QUERY} ORDER BY id;"

        return f"{_SELECT_ALL_ENTRIES_QUERY} WHERE {' AND '.join(conditions)} ORDER BY id;"


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
    aggregate_uuid UUID NOT NULL,
    aggregate_name TEXT NOT NULL,
    version INT NOT NULL,
    data BYTEA NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    transaction_uuid UUID NOT NULL DEFAULT uuid_nil(),
    UNIQUE (aggregate_uuid, version, transaction_uuid)
);
""".strip()

_INSERT_VALUES_QUERY = """
INSERT INTO aggregate_event (id, action, aggregate_uuid, aggregate_name, version, data, created_at, transaction_uuid)
VALUES (
    default,
    %(action)s,
    CASE %(aggregate_uuid)s WHEN uuid_nil() THEN uuid_generate_v4() ELSE %(aggregate_uuid)s END,
    %(aggregate_name)s,
    (
        SELECT (CASE WHEN %(version)s IS NULL THEN 1 + COALESCE(MAX(version), 0) ELSE %(version)s END)
        FROM aggregate_event
        WHERE aggregate_uuid = %(aggregate_uuid)s
          AND aggregate_name = %(aggregate_name)s
          AND transaction_uuid = (
            CASE (
                SELECT COUNT(*)
                FROM aggregate_event
                WHERE aggregate_uuid = %(aggregate_uuid)s
                    AND aggregate_name = %(aggregate_name)s
                    AND transaction_uuid =  %(transaction_uuid)s
            ) WHEN 0 THEN uuid_nil() ELSE %(transaction_uuid)s END
          )
    ),
    %(data)s,
    (CASE WHEN %(created_at)s IS NULL THEN NOW() ELSE %(created_at)s END),
    %(transaction_uuid)s
)
RETURNING id, aggregate_uuid, version, created_at;
""".strip()

_SELECT_ALL_ENTRIES_QUERY = """
SELECT aggregate_uuid, aggregate_name, version, data, id, action, created_at, transaction_uuid
FROM aggregate_event
""".strip()

_CREATE_TRANSACTION_STATUS_ENUM_QUERY = """
DO
$$
    BEGIN
        IF NOT EXISTS(SELECT *
                      FROM pg_type typ
                               INNER JOIN pg_namespace nsp
                                          ON nsp.oid = typ.typnamespace
                      WHERE nsp.nspname = current_schema()
                        AND typ.typname = 'transaction_status') THEN
            CREATE TYPE transaction_status AS ENUM ('created', 'pending', 'committed', 'rejected');
        END IF;
    END;
$$
LANGUAGE plpgsql;
""".strip()

_CREATE_TRANSACTION_TABLE_QUERY = """
CREATE TABLE IF NOT EXISTS aggregate_transaction (
    uuid UUID PRIMARY KEY,
    status TRANSACTION_STATUS NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
""".strip()


_INSERT_TRANSACTIONS_VALUES_QUERY = """
INSERT INTO aggregate_transaction (uuid, status, updated_at)
VALUES (
    CASE %(uuid)s WHEN uuid_nil() THEN uuid_generate_v4() ELSE %(uuid)s END,
    %(status)s,
    default
)
ON CONFLICT (uuid)
DO
   UPDATE SET status = %(status)s, updated_at = NOW()
RETURNING uuid, created_at, updated_at;
""".strip()

_SELECT_ALL_TRANSACTIONS_QUERY = """
SELECT uuid, status, created_at, updated_at
FROM aggregate_transaction
""".strip()
