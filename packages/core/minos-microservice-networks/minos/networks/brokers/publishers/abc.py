from __future__ import (
    annotations,
)

import logging

from psycopg2.sql import (
    SQL,
)

from minos.common import (
    PostgreSqlMinosDatabase,
)

logger = logging.getLogger(__name__)


class BrokerPublisherSetup(PostgreSqlMinosDatabase):
    """Broker Publisher Setup class."""

    async def _setup(self) -> None:
        await self._create_broker_table()

    async def _create_broker_table(self) -> None:
        await self.submit_query(_CREATE_TABLE_QUERY, lock=hash("producer_queue"))


_CREATE_TABLE_QUERY = SQL(
    "CREATE TABLE IF NOT EXISTS producer_queue ("
    "id BIGSERIAL NOT NULL PRIMARY KEY, "
    "topic VARCHAR(255) NOT NULL, "
    "data BYTEA NOT NULL, "
    "strategy VARCHAR(255) NOT NULL, "
    "retry INTEGER NOT NULL DEFAULT 0, "
    "created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), "
    "updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW())"
)
