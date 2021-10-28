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

from ...configuration import (
    MinosConfig,
)
from ...database import (
    PostgreSqlMinosDatabase,
)
from ..models import (
    Transaction,
)
from .abc import (
    TransactionRepository,
)


class PostgreSqlTransactionRepository(PostgreSqlMinosDatabase, TransactionRepository):
    """PostgreSql Transaction Repository class."""

    @classmethod
    def _from_config(cls, *args, config: MinosConfig, **kwargs) -> Optional[PostgreSqlTransactionRepository]:
        return cls(*args, **config.repository._asdict(), **kwargs)

    async def _setup(self):
        await self.submit_query('CREATE EXTENSION IF NOT EXISTS "uuid-ossp";')

        await self.submit_query(_CREATE_TRANSACTION_STATUS_ENUM_QUERY, lock=hash("aggregate_transaction_enum"))
        await self.submit_query(_CREATE_TRANSACTION_TABLE_QUERY, lock=hash("aggregate_transaction"))

    async def _submit(self, transaction: Transaction) -> None:
        params = {
            "uuid": transaction.uuid,
            "status": transaction.status,
            "event_offset": transaction.event_offset,
        }
        await self.submit_query_and_fetchone(
            _INSERT_TRANSACTIONS_VALUES_QUERY, params, lock=transaction.uuid.int & (1 << 32) - 1,
        )

    async def _select(self, **kwargs) -> AsyncIterator[Transaction]:
        query = self._build_select_query(**kwargs)
        async for row in self.submit_query_and_iter(query, kwargs, **kwargs):
            yield Transaction(*row, transaction_repository=self)

    # noinspection PyUnusedLocal
    @staticmethod
    def _build_select_query(
        uuid: Optional[UUID] = None,
        uuid_in: Optional[tuple[UUID]] = None,
        status: Optional[str] = None,
        status_in: Optional[tuple[str]] = None,
        event_offset: Optional[int] = None,
        event_offset_lt: Optional[int] = None,
        event_offset_gt: Optional[int] = None,
        event_offset_le: Optional[int] = None,
        event_offset_ge: Optional[int] = None,
        **kwargs,
    ) -> str:
        conditions = list()

        if uuid is not None:
            conditions.append("uuid = %(uuid)s")
        if uuid_in is not None:
            conditions.append("uuid IN %(uuid_in)s")
        if status is not None:
            conditions.append("status = %(status)s")
        if status_in is not None:
            conditions.append("status IN %(status_in)s")
        if event_offset is not None:
            conditions.append("event_offset = %(event_offset)s")
        if event_offset_lt is not None:
            conditions.append("event_offset < %(event_offset_lt)s")
        if event_offset_gt is not None:
            conditions.append("event_offset > %(event_offset_gt)s")
        if event_offset_le is not None:
            conditions.append("event_offset <= %(event_offset_le)s")
        if event_offset_ge is not None:
            conditions.append("event_offset >= %(event_offset_ge)s")

        if not conditions:
            return f"{_SELECT_ALL_TRANSACTIONS_QUERY} ORDER BY event_offset;"

        return f"{_SELECT_ALL_TRANSACTIONS_QUERY} WHERE {' AND '.join(conditions)} ORDER BY event_offset;"


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
            CREATE TYPE transaction_status AS ENUM ('pending', 'reserved', 'committed', 'rejected');
        END IF;
    END;
$$
LANGUAGE plpgsql;
""".strip()

_CREATE_TRANSACTION_TABLE_QUERY = """
CREATE TABLE IF NOT EXISTS aggregate_transaction (
    uuid UUID PRIMARY KEY,
    status TRANSACTION_STATUS NOT NULL,
    event_offset INTEGER,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
""".strip()

_INSERT_TRANSACTIONS_VALUES_QUERY = """
INSERT INTO aggregate_transaction (uuid, status, event_offset, updated_at)
VALUES (
    CASE %(uuid)s WHEN uuid_nil() THEN uuid_generate_v4() ELSE %(uuid)s END,
    %(status)s,
    %(event_offset)s,
    default
)
ON CONFLICT (uuid)
DO
   UPDATE SET status = %(status)s, event_offset = %(event_offset)s, updated_at = NOW()
RETURNING uuid, created_at, updated_at;
""".strip()

_SELECT_ALL_TRANSACTIONS_QUERY = """
SELECT uuid, status, event_offset
FROM aggregate_transaction
""".strip()
