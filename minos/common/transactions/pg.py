from __future__ import (
    annotations,
)

from typing import (
    Optional,
)

from ..configuration import (
    MinosConfig,
)
from ..database import (
    PostgreSqlMinosDatabase,
)
from .models import (
    Transaction,
)


class PostgreSqlTransactionRepository(PostgreSqlMinosDatabase):
    """PostgreSQL-based implementation of the repository class in ``Minos``."""

    @classmethod
    def _from_config(cls, *args, config: MinosConfig, **kwargs) -> Optional[PostgreSqlTransactionRepository]:
        return cls(*args, **config.repository._asdict(), **kwargs)

    async def _setup(self):
        """Setup miscellaneous repository thing.

        In the PostgreSQL case, configures the needed table to be used to store the data.

        :return: This method does not return anything.
        """
        await self.submit_query('CREATE EXTENSION IF NOT EXISTS "uuid-ossp";')

        await self.submit_query(_CREATE_TRANSACTION_STATUS_ENUM_QUERY, lock=hash("aggregate_transaction_enum"))
        await self.submit_query(_CREATE_TRANSACTION_TABLE_QUERY, lock=hash("aggregate_transaction"))

    async def submit(self, transaction: Transaction) -> None:
        """TODO"""
        await self.submit_query_and_fetchone(
            _INSERT_TRANSACTIONS_VALUES_QUERY,
            {"uuid": transaction.uuid, "status": transaction.status.value},
            lock=transaction.uuid.int & (1 << 32) - 1,
        )


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
