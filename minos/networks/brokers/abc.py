"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from abc import (
    ABC,
)
from typing import (
    NoReturn,
)

from psycopg2.sql import (
    SQL,
)

from minos.common import (
    MinosBroker,
    PostgreSqlMinosDatabase,
)


class BrokerSetup(PostgreSqlMinosDatabase):
    """Minos Broker Setup Class"""

    async def _setup(self) -> NoReturn:
        await self._create_broker_table()

    async def _create_broker_table(self) -> NoReturn:
        await self.submit_query(_CREATE_TABLE_QUERY, lock=hash("producer_queue"))


class Broker(MinosBroker, BrokerSetup, ABC):
    """Minos Broker Class."""

    ACTION: str

    async def send_bytes(self, topic: str, raw: bytes) -> int:
        """Send a sequence of bytes to the given topic.

        :param topic: Topic in which the bytes will be send.
        :param raw: Bytes sequence to be send.
        :return: The identifier of the message in the queue.
        """
        params = (topic, raw, 0, self.ACTION)
        raw = await self.submit_query_and_fetchone(_INSERT_ENTRY_QUERY, params)
        return raw[0]


_CREATE_TABLE_QUERY = SQL(
    "CREATE TABLE IF NOT EXISTS producer_queue ("
    "id BIGSERIAL NOT NULL PRIMARY KEY, "
    "topic VARCHAR(255) NOT NULL, "
    "model BYTEA NOT NULL, "
    "retry INTEGER NOT NULL, "
    "action VARCHAR(255) NOT NULL, "
    "creation_date TIMESTAMP NOT NULL, "
    "update_date TIMESTAMP NOT NULL)"
)

_INSERT_ENTRY_QUERY = SQL(
    "INSERT INTO producer_queue (topic, model, retry, action, creation_date, update_date) "
    "VALUES (%s, %s, %s, %s, NOW(), NOW()) "
    "RETURNING id"
)
