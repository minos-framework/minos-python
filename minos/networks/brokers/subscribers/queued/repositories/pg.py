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
from datetime import datetime
from functools import total_ordering
from typing import (
    NoReturn,
    Optional, Any,
)

from aiopg import (
    Cursor,
)
from cached_property import cached_property
from psycopg2.sql import (
    SQL,
    Identifier,
)

from minos.common import (
    MinosConfig,
    PostgreSqlMinosDatabase, current_datetime,
)

from .....utils import (
    consume_queue,
)
from ....messages import (
    BrokerMessage,
)
from .abc import (
    BrokerSubscriberRepository,
)

logger = logging.getLogger(__name__)


class PostgreSqlBrokerSubscriberRepository(BrokerSubscriberRepository, PostgreSqlMinosDatabase):
    """TODO"""
    _queue: PriorityQueue[PostgreSqlBrokerSubscriberRepositoryEntry]

    def __init__(self, topics: set[str], records: int, retry: int, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._topics = topics

        self._records = records
        self._retry = retry

        self._queue = PriorityQueue(maxsize=records)

        self._run_task = None

    @classmethod
    def _from_config(cls, config: MinosConfig, **kwargs) -> PostgreSqlBrokerSubscriberRepository:
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
        await self.submit_query(_CREATE_TABLE_QUERY, lock=hash("consumer_queue"))

    async def _start_run(self):
        if self._run_task is None:
            self._run_task = create_task(self._run())

    async def _stop_run(self):
        if self._run_task is not None:
            self._run_task.cancel()
            with suppress(TimeoutError, CancelledError):
                await wait_for(self._run_task, 0.5)
            self._run_task = None

    async def _flush_queue(self):
        while not self._queue.empty():
            entry = self._queue.get_nowait()
            await self.submit_query(_UPDATE_NOT_PROCESSED_QUERY, (entry.id,))
            self._queue.task_done()

    async def enqueue(self, message: BrokerMessage) -> None:
        """TODO

        :param message: TODO
        :return: TODO
        """
        logger.info(f"Enqueueing {message!r} message...")

        params = (message.topic, 0, message.avro_bytes)
        await self.submit_query_and_fetchone(_INSERT_QUERY, params)
        await self.submit_query(_NOTIFY_QUERY.format(Identifier(message.topic)))

    async def dequeue(self) -> BrokerMessage:
        """TODO

        :return: TODO
        """
        entry = await self._queue.get()

        try:
            try:
                message = entry.data
            except (CancelledError, Exception) as exc:
                await self.submit_query(_UPDATE_NOT_PROCESSED_QUERY, (entry.id,))
                raise exc
            await self.submit_query(_DELETE_PROCESSED_QUERY, (entry.id,))
        finally:
            self._queue.task_done()

        logger.info(f"Dequeuing {message!r} message...")

        return message

    async def _run(self, max_wait: Optional[float] = 60.0) -> NoReturn:
        async with self.cursor() as cursor:
            await self._listen_entries(cursor)
            try:
                while True:
                    await self._wait_for_entries(cursor, max_wait)
                    await self._dequeue_batch(cursor)
            finally:
                await self._unlisten_entries(cursor)

    async def _listen_entries(self, cursor: Cursor):
        for topic in self._topics:
            # noinspection PyTypeChecker
            await cursor.execute(_LISTEN_QUERY.format(Identifier(topic)))

    async def _unlisten_entries(self, cursor: Cursor) -> None:
        for topic in self._topics:
            # noinspection PyTypeChecker
            await cursor.execute(_UNLISTEN_QUERY.format(Identifier(topic)))

    async def _wait_for_entries(self, cursor: Cursor, max_wait: Optional[float]) -> None:
        while True:
            if await self._get_count(cursor):
                return

            with suppress(TimeoutError):
                return await wait_for(consume_queue(cursor.connection.notifies, self._records), max_wait)

    async def _get_count(self, cursor) -> int:
        if not len(self._topics):
            return 0
        await cursor.execute(_COUNT_NOT_PROCESSED_QUERY, (self._retry, tuple(self._topics)))
        count = (await cursor.fetchone())[0]
        return count

    async def _dequeue_batch(self, cursor: Cursor) -> None:
        async with cursor.begin():
            # noinspection PyTypeChecker
            await cursor.execute(
                _SELECT_NOT_PROCESSED_QUERY, (self._retry, tuple(self._topics), self._records)
            )
            result = await cursor.fetchall()

            if len(result):
                entries = [PostgreSqlBrokerSubscriberRepositoryEntry(*row) for row in result]

                # noinspection PyTypeChecker
                await cursor.execute(_MARK_PROCESSING_QUERY, (tuple(e.id for e in entries),))

                for entry in entries:
                    await self._queue.put(entry)


@total_ordering
class PostgreSqlBrokerSubscriberRepositoryEntry:
    """Handler Entry class."""

    def __init__(
        self,
        id: int,
        topic: str,
        partition: int,
        data_bytes: bytes,
        retry: int = 0,
        created_at: Optional[datetime] = None,
        updated_at: Optional[datetime] = None,
    ):
        if created_at is None or updated_at is None:
            now = current_datetime()
            if created_at is None:
                created_at = now
            if updated_at is None:
                updated_at = now

        self.id = id
        self.topic = topic
        self.partition = partition
        self.data_bytes = data_bytes
        self.retry = retry
        self.created_at = created_at
        self.updated_at = updated_at

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

    def __iter__(self):
        yield from (
            self.id,
            self.topic,
            self.partition,
            self.data_bytes,
            self.retry,
            self.created_at,
            self.updated_at,
        )

    def __repr__(self):
        return f"{type(self).__name__}({self.id!r}, {self.topic!r})"


_CREATE_TABLE_QUERY = SQL(
    "CREATE TABLE IF NOT EXISTS consumer_queue ("
    '"id" BIGSERIAL NOT NULL PRIMARY KEY, '
    '"topic" VARCHAR(255) NOT NULL, '
    '"partition" INTEGER,'
    '"data" BYTEA NOT NULL, '
    '"retry" INTEGER NOT NULL DEFAULT 0,'
    '"processing" BOOL NOT NULL DEFAULT FALSE, '
    '"created_at" TIMESTAMPTZ NOT NULL DEFAULT NOW(), '
    '"updated_at" TIMESTAMPTZ NOT NULL DEFAULT NOW())'
)

_INSERT_QUERY = SQL("INSERT INTO consumer_queue (topic, partition, data) VALUES (%s, %s, %s) RETURNING id")

_NOTIFY_QUERY = SQL("NOTIFY {}")

# noinspection SqlDerivedTableAlias
_COUNT_NOT_PROCESSED_QUERY = SQL(
    "SELECT COUNT(*) "
    "FROM (SELECT id FROM consumer_queue WHERE NOT processing AND retry < %s AND topic IN %s FOR UPDATE SKIP LOCKED) s"
)

_SELECT_NOT_PROCESSED_QUERY = SQL(
    "SELECT id, topic, partition, data, retry, created_at, updated_at "
    "FROM consumer_queue "
    "WHERE NOT processing AND retry < %s AND topic IN %s "
    "ORDER BY created_at "
    "LIMIT %s "
    "FOR UPDATE SKIP LOCKED"
)

_MARK_PROCESSING_QUERY = SQL("UPDATE consumer_queue SET processing = TRUE WHERE id IN %s")

_DELETE_PROCESSED_QUERY = SQL("DELETE FROM consumer_queue WHERE id = %s")

_UPDATE_NOT_PROCESSED_QUERY = SQL(
    "UPDATE consumer_queue SET processing = FALSE, retry = retry + 1, updated_at = NOW() WHERE id = %s"
)

_LISTEN_QUERY = SQL("LISTEN {}")

_UNLISTEN_QUERY = SQL("UNLISTEN {}")
