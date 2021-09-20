from psycopg2.sql import (
    SQL,
)

from minos.common import (
    PostgreSqlMinosDatabase,
)


class HandlerSetup(PostgreSqlMinosDatabase):
    """Minos Broker Setup Class"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    async def _setup(self) -> None:
        await self._create_event_queue_table()

    async def _create_event_queue_table(self) -> None:
        _CREATE_TABLE_QUERY = SQL(
            "CREATE TABLE IF NOT EXISTS consumer_queue ("
            '"id" BIGSERIAL NOT NULL PRIMARY KEY, '
            '"topic" VARCHAR(255) NOT NULL, '
            '"partition" INTEGER,'
            '"data" BYTEA NOT NULL, '
            '"retry" INTEGER NOT NULL DEFAULT 0,'
            '"created_at" TIMESTAMPTZ NOT NULL DEFAULT NOW(), '
            '"updated_at" TIMESTAMPTZ NOT NULL DEFAULT NOW())'
        )
        await self.submit_query(_CREATE_TABLE_QUERY, lock=hash("consumer_queue"))
