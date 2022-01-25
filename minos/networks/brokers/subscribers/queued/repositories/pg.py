from __future__ import (
    annotations,
)

import logging
from typing import (
    Any,
)

from aiopg import (
    Cursor,
)
from psycopg2.sql import (
    SQL,
    Identifier,
)

from ....collections import (
    PostgreSqlBrokerRepository,
)
from ....messages import (
    BrokerMessage,
)
from .abc import (
    BrokerSubscriberRepository,
)

logger = logging.getLogger(__name__)


class PostgreSqlBrokerSubscriberRepository(PostgreSqlBrokerRepository, BrokerSubscriberRepository):
    """PostgreSql Broker Subscriber Repository class."""

    _TABLE_NAME = "broker_subscriber_queue"

    _CREATE_TABLE_QUERY = SQL(
        f"CREATE TABLE IF NOT EXISTS {_TABLE_NAME} ("
        "id BIGSERIAL NOT NULL PRIMARY KEY, "
        "topic VARCHAR(255) NOT NULL, "
        "data BYTEA NOT NULL, "
        "retry INTEGER NOT NULL DEFAULT 0, "
        "processing BOOL NOT NULL DEFAULT FALSE, "
        "created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), "
        "updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW())"
    )

    _INSERT_QUERY = SQL(f"INSERT INTO {_TABLE_NAME} (topic, data) VALUES (%s, %s) RETURNING id")

    _COUNT_NOT_PROCESSED_QUERY = SQL(
        "SELECT COUNT(*) "
        f"FROM (SELECT id FROM {_TABLE_NAME} WHERE "
        "NOT processing AND retry < %s AND topic IN %s FOR UPDATE SKIP LOCKED) s"
    )

    _SELECT_NOT_PROCESSED_QUERY = SQL(
        "SELECT id, data "
        f"FROM {_TABLE_NAME} "
        "WHERE NOT processing AND retry < %s AND topic IN %s "
        "ORDER BY created_at "
        "LIMIT %s "
        "FOR UPDATE SKIP LOCKED"
    )

    _MARK_PROCESSING_QUERY = SQL(f"UPDATE {_TABLE_NAME} SET processing = TRUE WHERE id IN %s")

    _DELETE_PROCESSED_QUERY = SQL(f"DELETE FROM {_TABLE_NAME} WHERE id = %s")

    _UPDATE_NOT_PROCESSED_QUERY = SQL(
        f"UPDATE {_TABLE_NAME} SET processing = FALSE, retry = retry + 1, updated_at = NOW() WHERE id = %s"
    )

    _LISTEN_QUERY = SQL("LISTEN {}")

    _UNLISTEN_QUERY = SQL("UNLISTEN {}")

    _NOTIFY_QUERY = SQL("NOTIFY {}")

    def __init__(self, topics: set[str], *args, **kwargs):
        super().__init__(topics, *args, **kwargs)

    async def _enqueue(self, message: BrokerMessage) -> None:
        params = (message.topic, message.avro_bytes)
        await self.submit_query_and_fetchone(self._INSERT_QUERY, params)
        await self.submit_query(self._NOTIFY_QUERY.format(Identifier(message.topic)))

    async def _listen_entries(self, cursor: Cursor):
        for topic in self.topics:
            # noinspection PyTypeChecker
            await cursor.execute(self._LISTEN_QUERY.format(Identifier(topic)))

    async def _unlisten_entries(self, cursor: Cursor) -> None:
        for topic in self.topics:
            # noinspection PyTypeChecker
            await cursor.execute(self._UNLISTEN_QUERY.format(Identifier(topic)))

    async def _get_count(self, cursor) -> int:
        # noinspection PyTypeChecker
        await cursor.execute(self._COUNT_NOT_PROCESSED_QUERY, (self._retry, tuple(self.topics)))
        count = (await cursor.fetchone())[0]
        return count

    async def _dequeue_rows(self, cursor: Cursor) -> list[Any]:
        # noinspection PyTypeChecker
        await cursor.execute(self._SELECT_NOT_PROCESSED_QUERY, (self._retry, tuple(self.topics), self._records))
        return await cursor.fetchall()
