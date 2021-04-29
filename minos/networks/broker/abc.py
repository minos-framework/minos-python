"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from abc import ABC
from datetime import datetime
from typing import (
    NoReturn,
)

import aiopg
from minos.common import (
    MinosBaseBroker,
    MinosSetup,
)


class MinosBrokerSetup(MinosSetup):
    """TODO"""

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
        await self.broker_table_creation()

    async def broker_table_creation(self):
        """TODO

        :return:TODO
        """
        async with self._connection() as connect:
            async with connect.cursor() as cur:
                await cur.execute(
                    'CREATE TABLE IF NOT EXISTS "producer_queue" ('
                    '"id" BIGSERIAL NOT NULL PRIMARY KEY, '
                    '"topic" VARCHAR(255) NOT NULL, '
                    '"model" BYTEA NOT NULL, '
                    '"retry" INTEGER NOT NULL, '
                    '"action" VARCHAR(255) NOT NULL, '
                    '"creation_date" TIMESTAMP NOT NULL, '
                    '"update_date" TIMESTAMP NOT NULL);'
                )

    def _connection(self):
        return aiopg.connect(
            host=self.host, port=self.port, dbname=self.database, user=self.user, password=self.password,
        )


class MinosBroker(MinosBaseBroker, MinosBrokerSetup, ABC):
    """TODO"""
    ACTION: str

    def __init__(self, topic: str, *args, **kwargs):
        MinosBaseBroker.__init__(self, topic)
        MinosBrokerSetup.__init__(self, *args, **kwargs)

    async def _send_bytes(self, topic: str, raw: bytes) -> (int, int):
        async with self._connection() as connect:
            async with connect.cursor() as cur:
                await cur.execute(
                    "INSERT INTO producer_queue ("
                    "topic, model, retry, action, creation_date, update_date"
                    ") VALUES (%s, %s, %s, %s, %s, %s) RETURNING id;",
                    (topic, raw, 0, self.ACTION, datetime.now(), datetime.now()),
                )

                queue_id = await cur.fetchone()
                affected_rows = cur.rowcount

        return affected_rows, queue_id[0]
