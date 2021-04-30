"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from abc import (
    ABC,
)
from datetime import (
    datetime,
)
from typing import (
    NoReturn,
)

import aiopg
from minos.common import (
    MinosSetup,
)


class MinosEventSetup(MinosSetup):
    """Minos Broker Setup Class"""

    def __init__(
        self,
        *args,
        host: str = None,
        port: int = None,
        database: str = None,
        user: str = None,
        password: str = None,
        **kwargs
    ):
        super().__init__(*args, **kwargs)

        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password

    async def _setup(self) -> NoReturn:
        await self._create_event_queue_table()

    async def _create_event_queue_table(self) -> NoReturn:
        async with self._connection() as connect:
            async with connect.cursor() as cur:
                await cur.execute(
                    'CREATE TABLE IF NOT EXISTS "event_queue" ('
                    '"id" BIGSERIAL NOT NULL PRIMARY KEY, '
                    '"topic" VARCHAR(255) NOT NULL, '
                    '"partition_id" INTEGER,'
                    '"binary_data" BYTEA NOT NULL, '
                    '"creation_date" TIMESTAMP NOT NULL);'
                )

    def _connection(self):
        return aiopg.connect(
            host=self.host, port=self.port, dbname=self.database, user=self.user, password=self.password,
        )
