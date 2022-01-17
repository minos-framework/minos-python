from __future__ import (
    annotations,
)

import logging
from asyncio import (
    wait_for,
)
from collections.abc import (
    AsyncIterator,
)
from contextlib import (
    suppress,
)
from functools import (
    cached_property,
)
from typing import (
    Optional,
)

from aiopg import (
    Cursor,
)
from psycopg2.sql import (
    SQL,
)

from minos.common import (
    MinosConfig,
    PostgreSqlMinosDatabase,
)

from ......utils import (
    consume_queue,
)
from .....messages import (
    BrokerMessage,
)
from ..abc import (
    BrokerPublisherRepository,
)

logger = logging.getLogger(__name__)


class PostgreSqlBrokerPublisherRepository(BrokerPublisherRepository, PostgreSqlMinosDatabase):
    """PostgreSql Broker Publisher Repository class."""

    def __init__(self, *args, retry: int, records: int, **kwargs):
        super().__init__(*args, **kwargs)
        self.retry = retry
        self.records = records

    @classmethod
    def _from_config(cls, config: MinosConfig, **kwargs) -> PostgreSqlBrokerPublisherRepository:
        # noinspection PyProtectedMember
        return cls(**config.broker.queue._asdict(), **kwargs)

    async def _setup(self) -> None:
        await self._create_broker_table()

    async def _create_broker_table(self) -> None:
        await self.submit_query(_CREATE_TABLE_QUERY, lock=hash("producer_queue"))

    async def enqueue(self, message: BrokerMessage) -> None:
        """Enqueue method."""
        # TODO

        params = (message.topic, message.avro_bytes, message.strategy)
        await self.submit_query_and_fetchone(_INSERT_ENTRY_QUERY, params)
        await self.submit_query(_NOTIFY_QUERY)

    async def dequeue_all(self, max_wait: Optional[float] = 60.0) -> AsyncIterator[BrokerMessage]:
        """Dispatch the items in the publishing queue forever.

        :param max_wait: Maximum seconds to wait for notifications. If ``None`` the wait is performed until infinity.
        :return: This method does not return anything.
        """
        async with self.cursor() as cursor:
            await cursor.execute(self._queries["listen"])
            try:
                while True:
                    await self._wait_for_entries(cursor, max_wait)
                    async for message in self._dequeue_batch(cursor):
                        yield message
            finally:
                await cursor.execute(self._queries["unlisten"])

    async def _wait_for_entries(self, cursor: Cursor, max_wait: Optional[float]) -> None:
        while True:
            if await self._get_count(cursor):
                return

            with suppress(TimeoutError):
                return await wait_for(consume_queue(cursor.connection.notifies, self.records), max_wait)

    async def _get_count(self, cursor) -> int:
        await cursor.execute(self._queries["count_not_processed"], (self.retry,))
        count = (await cursor.fetchone())[0]
        return count

    async def _dequeue_batch(self, cursor: Cursor) -> AsyncIterator[BrokerMessage]:
        async with cursor.begin():
            await cursor.execute(self._queries["select_not_processed"], (self.retry, self.records))
            rows = await cursor.fetchall()
            for row in rows:

                try:
                    yield self._dispatch_one(row)
                    ok = True
                except Exception as exc:
                    logger.warning(f"There was an exception while trying to dequeue the row with {row[0]} id: {exc}")
                    ok = False

                if ok:
                    await cursor.execute(self._queries["delete_processed"], (row[0],))
                else:
                    await cursor.execute(self._queries["update_not_processed"], (row[0],))

    @staticmethod
    def _dispatch_one(row: tuple) -> BrokerMessage:
        bytes_ = row[2]
        return BrokerMessage.from_avro_bytes(bytes_)

    @cached_property
    def _queries(self) -> dict[str, str]:
        # noinspection PyTypeChecker
        return {
            "listen": _LISTEN_QUERY,
            "unlisten": _UNLISTEN_QUERY,
            "count_not_processed": _COUNT_NOT_PROCESSED_QUERY,
            "select_not_processed": _SELECT_NOT_PROCESSED_QUERY,
            "delete_processed": _DELETE_PROCESSED_QUERY,
            "update_not_processed": _UPDATE_NOT_PROCESSED_QUERY,
        }


_CREATE_TABLE_QUERY = SQL(
    "CREATE TABLE IF NOT EXISTS producer_queue ("
    "id BIGSERIAL NOT NULL PRIMARY KEY, "
    "topic VARCHAR(255) NOT NULL, "
    "data BYTEA NOT NULL, "
    "strategy VARCHAR(255) NOT NULL, "
    "retry INTEGER NOT NULL DEFAULT 0, "
    "created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), "
    "updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW())"
)

_INSERT_ENTRY_QUERY = SQL("INSERT INTO producer_queue (topic, data, strategy) VALUES (%s, %s, %s) RETURNING id")

_NOTIFY_QUERY = SQL("NOTIFY producer_queue")

# noinspection SqlDerivedTableAlias
_COUNT_NOT_PROCESSED_QUERY = SQL(
    "SELECT COUNT(*) FROM (SELECT id FROM producer_queue WHERE retry < %s FOR UPDATE SKIP LOCKED) s"
)

_SELECT_NOT_PROCESSED_QUERY = SQL(
    "SELECT * "
    "FROM producer_queue "
    "WHERE retry < %s "
    "ORDER BY created_at "
    "LIMIT %s "
    "FOR UPDATE "
    "SKIP LOCKED"
)

_DELETE_PROCESSED_QUERY = SQL("DELETE FROM producer_queue WHERE id = %s")

_UPDATE_NOT_PROCESSED_QUERY = SQL("UPDATE producer_queue SET retry = retry + 1, updated_at = NOW() WHERE id = %s")

_LISTEN_QUERY = SQL("LISTEN producer_queue")

_UNLISTEN_QUERY = SQL("UNLISTEN producer_queue")
