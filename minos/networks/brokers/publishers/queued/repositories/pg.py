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
from collections.abc import (
    Iterator,
)
from contextlib import (
    suppress,
)
from functools import (
    total_ordering,
)
from typing import (
    Any,
    Optional,
)

from aiopg import (
    Cursor,
)
from cached_property import (
    cached_property,
)
from psycopg2.sql import (
    SQL,
)

from minos.common import (
    MinosConfig,
    PostgreSqlMinosDatabase,
)

from .....utils import (
    consume_queue,
)
from ....messages import (
    BrokerMessage,
)
from .abc import (
    BrokerPublisherRepository,
)

logger = logging.getLogger(__name__)


class PostgreSqlBrokerPublisherRepository(BrokerPublisherRepository, PostgreSqlMinosDatabase):
    """PostgreSql Broker Publisher Repository class."""

    _queue: PriorityQueue[PostgreSqlBrokerPublisherRepositoryEntry]

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
        await self._flush_queue()
        await super()._destroy()

    async def _create_broker_table(self) -> None:
        await self.submit_query(_CREATE_TABLE_QUERY, lock=hash("broker_publisher_queue"))

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

        params = (message.topic, message.avro_bytes)
        await self.submit_query_and_fetchone(_INSERT_QUERY, params)
        await self.submit_query(_NOTIFY_QUERY)

    async def _flush_queue(self):
        while not self._queue.empty():
            entry = self._queue.get_nowait()
            await self.submit_query(_UPDATE_NOT_PROCESSED_QUERY, (entry.id_,))
            self._queue.task_done()

    async def dequeue(self) -> BrokerMessage:
        """Dequeue method."""
        entry = await self._queue.get()

        try:
            try:
                message = entry.data
            except (CancelledError, Exception) as exc:
                await self.submit_query(_UPDATE_NOT_PROCESSED_QUERY, (entry.id_,))
                if isinstance(exc, CancelledError):
                    raise exc
                return await self.dequeue()

            await self.submit_query(_DELETE_PROCESSED_QUERY, (entry.id_,))
        finally:
            self._queue.task_done()

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

            if not len(rows):
                return

            entries = [PostgreSqlBrokerPublisherRepositoryEntry(*row) for row in rows]

            # noinspection PyTypeChecker
            await cursor.execute(_MARK_PROCESSING_QUERY, (tuple(e.id_ for e in entries),))

            for entry in entries:
                await self._queue.put(entry)


@total_ordering
class PostgreSqlBrokerPublisherRepositoryEntry:
    """TODO"""

    def __init__(self, id_: int, data_bytes: bytes):
        self.id_ = id_
        self.data_bytes = data_bytes

    @cached_property
    def data(self) -> BrokerMessage:
        """Get the data.

        :return: A ``Model`` inherited instance.
        """
        return BrokerMessage.from_avro_bytes(self.data_bytes)

    def __lt__(self, other: Any) -> bool:
        # noinspection PyBroadException
        try:
            return isinstance(other, type(self)) and self.data < other.data
        except Exception:
            return False

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, type(self)) and tuple(self) == tuple(other)

    def __iter__(self) -> Iterator[Any]:
        yield from (
            self.id_,
            self.data_bytes,
        )

    def __repr__(self):
        return f"{type(self).__name__}({self.id_!r})"


_CREATE_TABLE_QUERY = SQL(
    "CREATE TABLE IF NOT EXISTS broker_publisher_queue ("
    "id BIGSERIAL NOT NULL PRIMARY KEY, "
    "topic VARCHAR(255) NOT NULL, "
    "data BYTEA NOT NULL, "
    "retry INTEGER NOT NULL DEFAULT 0, "
    "processing BOOL NOT NULL DEFAULT FALSE, "
    "created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), "
    "updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW())"
)

_INSERT_QUERY = SQL("INSERT INTO broker_publisher_queue (topic, data) VALUES (%s, %s) RETURNING id")

_NOTIFY_QUERY = SQL("NOTIFY broker_publisher_queue")

# noinspection SqlDerivedTableAlias
_COUNT_NOT_PROCESSED_QUERY = SQL(
    "SELECT COUNT(*) FROM (SELECT id FROM broker_publisher_queue WHERE retry < %s FOR UPDATE SKIP LOCKED) s"
)

_SELECT_NOT_PROCESSED_QUERY = SQL(
    "SELECT id, data "
    "FROM broker_publisher_queue "
    "WHERE NOT processing AND retry < %s "
    "ORDER BY created_at "
    "LIMIT %s "
    "FOR UPDATE "
    "SKIP LOCKED"
)

_MARK_PROCESSING_QUERY = SQL("UPDATE broker_publisher_queue SET processing = TRUE WHERE id IN %s")

_DELETE_PROCESSED_QUERY = SQL("DELETE FROM broker_publisher_queue WHERE id = %s")

_UPDATE_NOT_PROCESSED_QUERY = SQL(
    "UPDATE broker_publisher_queue SET retry = retry + 1, updated_at = NOW() WHERE id = %s"
)

_LISTEN_QUERY = SQL("LISTEN broker_publisher_queue")

_UNLISTEN_QUERY = SQL("UNLISTEN broker_publisher_queue")
