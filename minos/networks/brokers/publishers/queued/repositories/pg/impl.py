from __future__ import (
    annotations,
)

import logging
from asyncio import (
    CancelledError,
    PriorityQueue,
    TimeoutError,
    create_task,
    wait_for,
)
from contextlib import (
    suppress,
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

        self._queue = PriorityQueue(maxsize=records)

        self._run_task = None

    @classmethod
    def _from_config(cls, config: MinosConfig, **kwargs) -> PostgreSqlBrokerPublisherRepository:
        # noinspection PyProtectedMember
        return cls(**config.broker.queue._asdict(), **kwargs)

    async def _setup(self) -> None:
        await super()._setup()
        await self._create_broker_table()
        await self._start_run()

    async def _destroy(self) -> None:
        await self._stop_run()
        await super()._destroy()

    async def _create_broker_table(self) -> None:
        await self.submit_query(_CREATE_TABLE_QUERY, lock=hash("producer_queue"))

    async def _start_run(self) -> None:
        if self._run_task is None:
            self._run_task = create_task(self._run())

    async def _stop_run(self) -> None:
        if self._run_task is not None:
            self._run_task.cancel()
            with suppress(TimeoutError, CancelledError):
                await wait_for(self._run_task, 0.5)
            self._run_task = None

    async def enqueue(self, message: BrokerMessage) -> None:
        """Enqueue method."""
        logger.info(f"Enqueuing {message!r} message...")

        params = (message.topic, message.avro_bytes, message.strategy)
        await self.submit_query_and_fetchone(_INSERT_ENTRY_QUERY, params)
        await self.submit_query(_NOTIFY_QUERY)

    async def dequeue(self) -> BrokerMessage:
        """Dequeue method."""
        message = await self._queue.get()
        logger.info(f"Dequeuing {message!r} message...")
        return message

    async def _run(self, max_wait: Optional[float] = 60.0) -> None:
        async with self.cursor() as cursor:
            # noinspection PyTypeChecker
            await cursor.execute(_LISTEN_QUERY)
            try:
                while True:
                    await self._wait_for_entries(cursor, max_wait)
                    await self._dequeue_batch(cursor)
            finally:
                # noinspection PyTypeChecker
                await cursor.execute(_UNLISTEN_QUERY)

    async def _wait_for_entries(self, cursor: Cursor, max_wait: Optional[float]) -> None:
        while True:
            if await self._get_count(cursor):
                return

            with suppress(TimeoutError):
                return await wait_for(consume_queue(cursor.connection.notifies, self.records), max_wait)

    async def _get_count(self, cursor) -> int:
        await cursor.execute(_COUNT_NOT_PROCESSED_QUERY, (self.retry,))
        count = (await cursor.fetchone())[0]
        return count

    async def _dequeue_batch(self, cursor: Cursor) -> None:
        async with cursor.begin():
            # noinspection PyTypeChecker
            await cursor.execute(_SELECT_NOT_PROCESSED_QUERY, (self.retry, self.records))
            rows = await cursor.fetchall()
            for row in rows:

                try:
                    await self._dispatch_one(row)
                    ok = True
                except Exception as exc:
                    logger.warning(f"There was an exception while trying to dequeue the row with {row[0]} id: {exc}")
                    ok = False

                if ok:
                    # noinspection PyTypeChecker
                    await cursor.execute(_DELETE_PROCESSED_QUERY, (row[0],))
                else:
                    # noinspection PyTypeChecker
                    await cursor.execute(_UPDATE_NOT_PROCESSED_QUERY, (row[0],))

    async def _dispatch_one(self, row: tuple) -> None:
        bytes_ = row[2]
        message = BrokerMessage.from_avro_bytes(bytes_)
        await self._queue.put(message)


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
