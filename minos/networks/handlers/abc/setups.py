"""minos.networks.handlers.abc.setups module."""

from typing import (
    NoReturn,
)

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

    async def _setup(self) -> NoReturn:
        await self._create_event_queue_table()

    async def _create_event_queue_table(self) -> NoReturn:
        _CREATE_TABLE_QUERY = SQL(
            "CREATE TABLE IF NOT EXISTS consumer_queue ("
            '"id" BIGSERIAL NOT NULL PRIMARY KEY, '
            '"topic" VARCHAR(255) NOT NULL, '
            '"partition_id" INTEGER,'
            '"binary_data" BYTEA NOT NULL, '
            '"retry" INTEGER NOT NULL DEFAULT 0,'
            '"creation_date" TIMESTAMP NOT NULL)'
        )
        await self.submit_query(_CREATE_TABLE_QUERY, lock=hash("consumer_queue"))
