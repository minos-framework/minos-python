"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from typing import (
    NoReturn,
)

from minos.common import (
    PostgreSqlMinosDatabase,
)


class HandlerSetup(PostgreSqlMinosDatabase):
    """Minos Broker Setup Class"""

    def __init__(self, table_name: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.table_name = table_name

    async def _setup(self) -> NoReturn:
        await self._create_event_queue_table()

    async def _create_event_queue_table(self) -> NoReturn:
        await self.submit_query(
            'CREATE TABLE IF NOT EXISTS "%s" ('
            '"id" BIGSERIAL NOT NULL PRIMARY KEY, '
            '"topic" VARCHAR(255) NOT NULL, '
            '"partition_id" INTEGER,'
            '"binary_data" BYTEA NOT NULL, '
            '"creation_date" TIMESTAMP NOT NULL);' % (self.table_name)
        )
