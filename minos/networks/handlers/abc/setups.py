"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from typing import (
    NoReturn,
)

from psycopg2.sql import (
    SQL,
    Identifier,
)

from minos.common import (
    PostgreSqlMinosDatabase,
)


class HandlerSetup(PostgreSqlMinosDatabase):
    """Minos Broker Setup Class"""

    TABLE_NAME: str

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    async def _setup(self) -> NoReturn:
        await self._create_event_queue_table()

    async def _create_event_queue_table(self) -> NoReturn:
        _CREATE_TABLE_QUERY = SQL(
            "CREATE TABLE IF NOT EXISTS {} ("
            '"id" BIGSERIAL NOT NULL PRIMARY KEY, '
            '"topic" VARCHAR(255) NOT NULL, '
            '"partition_id" INTEGER,'
            '"binary_data" BYTEA NOT NULL, '
            '"retry" INTEGER NOT NULL DEFAULT 0,'
            '"creation_date" TIMESTAMP NOT NULL)'
        )

        with (await self.cursor()) as cursor:
            await cursor.execute("select pg_advisory_lock(%s)", (hash(self.TABLE_NAME),))
            try:
                await cursor.execute(_CREATE_TABLE_QUERY.format(Identifier(self.TABLE_NAME)))
            finally:
                await cursor.execute("select pg_advisory_unlock(%s)", (hash(self.TABLE_NAME),))
