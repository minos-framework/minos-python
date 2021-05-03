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

from minos.common import (
    MinosBaseBroker,
    PostgreSqlMinosDatabase,
)


class MinosBrokerSetup(PostgreSqlMinosDatabase):
    """Minos Broker Setup Class"""

    async def _setup(self) -> NoReturn:
        await self._create_broker_table()

    async def _create_broker_table(self) -> NoReturn:
        await self.submit_query(_CREATE_TABLE_QUERY)


class MinosBroker(MinosBaseBroker, MinosBrokerSetup, ABC):
    """Minos Broker Class."""

    ACTION: str

    def __init__(self, topic: str, *args, **kwargs):
        MinosBaseBroker.__init__(self, topic)
        MinosBrokerSetup.__init__(self, *args, **kwargs)

    async def _send_bytes(self, topic: str, raw: bytes) -> int:
        params = (topic, raw, 0, self.ACTION, datetime.now(), datetime.now())
        raw = await self.submit_query_and_fetchone(_INSERT_ENTRY_QUERY, params)
        return raw[0]


_CREATE_TABLE_QUERY = """
CREATE TABLE IF NOT EXISTS producer_queue (
    id BIGSERIAL NOT NULL PRIMARY KEY,
    topic VARCHAR(255) NOT NULL,
    model BYTEA NOT NULL,
    retry INTEGER NOT NULL,
    action VARCHAR(255) NOT NULL,
    creation_date TIMESTAMP NOT NULL,
    update_date TIMESTAMP NOT NULL
);
""".strip()

_INSERT_ENTRY_QUERY = """
INSERT INTO producer_queue (topic, model, retry, action, creation_date, update_date)
VALUES (%s, %s, %s, %s, %s, %s)
RETURNING id;
""".strip()
