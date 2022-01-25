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
)

from ....collections import (
    PostgreSqlBrokerRepository,
)
from ....messages import (
    BrokerMessage,
)
from .abc import (
    BrokerPublisherQueue,
)

logger = logging.getLogger(__name__)


class PostgreSqlBrokerPublisherQueue(PostgreSqlBrokerRepository, BrokerPublisherQueue):
    """PostgreSql Broker Publisher Queue class."""

    _TABLE_NAME = "broker_publisher_queue"

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
        f"SELECT COUNT(*) FROM (SELECT id FROM {_TABLE_NAME} WHERE retry < %s FOR UPDATE SKIP LOCKED) s"
    )

    _SELECT_NOT_PROCESSED_QUERY = SQL(
        "SELECT id, data "
        f"FROM {_TABLE_NAME} "
        "WHERE NOT processing AND retry < %s "
        "ORDER BY created_at "
        "LIMIT %s "
        "FOR UPDATE "
        "SKIP LOCKED"
    )

    _MARK_PROCESSING_QUERY = SQL(f"UPDATE {_TABLE_NAME} SET processing = TRUE WHERE id IN %s")

    _DELETE_PROCESSED_QUERY = SQL(f"DELETE FROM {_TABLE_NAME} WHERE id = %s")

    _UPDATE_NOT_PROCESSED_QUERY = SQL(f"UPDATE {_TABLE_NAME} SET retry = retry + 1, updated_at = NOW() WHERE id = %s")

    _LISTEN_QUERY = SQL(f"LISTEN {_TABLE_NAME}")

    _UNLISTEN_QUERY = SQL(f"UNLISTEN {_TABLE_NAME}")

    _NOTIFY_QUERY = SQL(f"NOTIFY {_TABLE_NAME}")

    async def _enqueue(self, message: BrokerMessage) -> None:
        """Enqueue method."""
        params = (message.topic, message.avro_bytes)
        await self.submit_query_and_fetchone(self._INSERT_QUERY, params)
        await self.submit_query(self._NOTIFY_QUERY)

    async def _listen_entries(self, cursor: Cursor) -> None:
        # noinspection PyTypeChecker
        await cursor.execute(self._LISTEN_QUERY)

    async def _unlisten_entries(self, cursor: Cursor) -> None:
        if not cursor.closed:
            # noinspection PyTypeChecker
            await cursor.execute(self._UNLISTEN_QUERY)

    async def _get_count(self, cursor: Cursor) -> int:
        # noinspection PyTypeChecker
        await cursor.execute(self._COUNT_NOT_PROCESSED_QUERY, (self._retry,))
        count = (await cursor.fetchone())[0]
        return count

    async def _dequeue_rows(self, cursor: Cursor) -> list[Any]:
        # noinspection PyTypeChecker
        await cursor.execute(self._SELECT_NOT_PROCESSED_QUERY, (self._retry, self._records))
        return await cursor.fetchall()
