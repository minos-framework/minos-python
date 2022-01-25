from __future__ import (
    annotations,
)

import logging
from asyncio import (
    CancelledError,
    PriorityQueue,
    QueueEmpty,
    TimeoutError,
    create_task,
    wait_for,
)
from contextlib import (
    suppress,
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
)

from .....utils import (
    consume_queue,
)
from ....collections import (
    PostgreSqlBrokerRepository,
)
from ....messages import (
    BrokerMessage,
)
from .abc import (
    BrokerPublisherRepository,
)

logger = logging.getLogger(__name__)


class PostgreSqlBrokerPublisherRepository(PostgreSqlBrokerRepository, BrokerPublisherRepository):
    """PostgreSql Broker Publisher Repository class."""

    _queue: PriorityQueue[_Entry]

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

    def __init__(self, *args, retry: int, records: int, **kwargs):
        super().__init__(*args, **kwargs)
        self._retry = retry
        self._records = records

        self._queue = PriorityQueue(maxsize=records)

        self._run_task = None

    @classmethod
    def _from_config(cls, config: MinosConfig, **kwargs) -> PostgreSqlBrokerPublisherRepository:
        # noinspection PyProtectedMember
        return cls(**config.broker.queue._asdict(), **kwargs)

    async def _setup(self) -> None:
        await super()._setup()
        await self._create_table()
        await self._start_run()

    async def _destroy(self) -> None:
        await self._stop_run()
        await self._flush_queue()
        await super()._destroy()

    async def _create_table(self) -> None:
        await self.submit_query(self._CREATE_TABLE_QUERY, lock=hash("broker_publisher_queue"))

    async def _start_run(self) -> None:
        if self._run_task is None:
            self._run_task = create_task(self._run())

    async def _stop_run(self) -> None:
        if self._run_task is not None:
            task = self._run_task
            self._run_task = None

            task.cancel()
            with suppress(TimeoutError, CancelledError):
                await wait_for(task, 0.5)

    async def _flush_queue(self):
        while True:
            try:
                entry = self._queue.get_nowait()
            except QueueEmpty:
                break
            await self.submit_query(self._UPDATE_NOT_PROCESSED_QUERY, (entry.id_,))
            self._queue.task_done()

    async def _enqueue(self, message: BrokerMessage) -> None:
        """Enqueue method."""
        params = (message.topic, message.avro_bytes)
        await self.submit_query_and_fetchone(self._INSERT_QUERY, params)
        await self.submit_query(self._NOTIFY_QUERY)

    async def _dequeue(self) -> BrokerMessage:
        while True:
            entry = await self._queue.get()
            try:
                # noinspection PyBroadException
                try:
                    message = entry.data
                except Exception as exc:
                    logger.warning(
                        f"There was a problem while trying to deserialize the entry with {entry.id_!r} id: {exc}"
                    )
                    await self.submit_query(self._UPDATE_NOT_PROCESSED_QUERY, (entry.id_,))
                    continue

                await self.submit_query(self._DELETE_PROCESSED_QUERY, (entry.id_,))
                return message
            finally:
                self._queue.task_done()

    async def _run(self, max_wait: Optional[float] = 60.0) -> None:
        async with self.cursor() as cursor:
            # noinspection PyTypeChecker
            await cursor.execute(self._LISTEN_QUERY)
            try:
                while self._run_task is not None:
                    await self._wait_for_entries(cursor, max_wait)
                    await self._dequeue_batch(cursor)
            finally:
                if not cursor.closed:
                    # noinspection PyTypeChecker
                    await cursor.execute(self._UNLISTEN_QUERY)

    async def _wait_for_entries(self, cursor: Cursor, max_wait: Optional[float]) -> None:
        while True:
            if await self._get_count(cursor):
                return

            with suppress(TimeoutError):
                return await wait_for(consume_queue(cursor.connection.notifies, self._records), max_wait)

    async def _get_count(self, cursor) -> int:
        await cursor.execute(self._COUNT_NOT_PROCESSED_QUERY, (self._retry,))
        count = (await cursor.fetchone())[0]
        return count

    async def _dequeue_batch(self, cursor: Cursor) -> None:
        async with cursor.begin():
            # noinspection PyTypeChecker
            await cursor.execute(self._SELECT_NOT_PROCESSED_QUERY, (self._retry, self._records))
            rows = await cursor.fetchall()

            if not len(rows):
                return

            entries = [_Entry(*row) for row in rows]

            # noinspection PyTypeChecker
            await cursor.execute(self._MARK_PROCESSING_QUERY, (tuple(entry.id_ for entry in entries),))

            for entry in entries:
                await self._queue.put(entry)


class _Entry:
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
