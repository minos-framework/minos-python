from __future__ import (
    annotations,
)

import logging
from abc import (
    ABC,
    abstractmethod,
)
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
    NoReturn,
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
    Config,
    PostgreSqlMinosDatabase,
)

from ....utils import (
    Builder,
    consume_queue,
)
from ...messages import (
    BrokerMessage,
)
from .abc import (
    BrokerQueue,
)

logger = logging.getLogger(__name__)


class PostgreSqlBrokerQueue(BrokerQueue, PostgreSqlMinosDatabase):
    """PostgreSql Broker Queue class."""

    _queue: PriorityQueue[_Entry]

    def __init__(
        self,
        *args,
        query_factory: PostgreSqlBrokerQueueQueryFactory,
        retry: Optional[int] = None,
        records: Optional[int] = None,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)

        if retry is None:
            retry = 2
        if records is None:
            records = 1000

        self._query_factory = query_factory
        self._retry = retry
        self._records = records

        self._queue = PriorityQueue(maxsize=records)

        self._run_task = None

    @property
    def retry(self) -> int:
        """Get the retry value.

        :return: A ``int`` value.
        """
        return self._retry

    @property
    def records(self) -> int:
        """Get the records value.

        :return: A ``int`` value.
        """
        return self._records

    @property
    def query_factory(self) -> PostgreSqlBrokerQueueQueryFactory:
        """Get the query factory.

        :return: A ``PostgreSqlBrokerQueueQueryFactory`` instance.
        """
        return self._query_factory

    @classmethod
    def _from_config(cls, config: Config, **kwargs) -> PostgreSqlBrokerQueue:
        broker_interface = config.get_interface_by_name("broker")
        queue_config = broker_interface.get("common", dict()).get("queue", dict())
        database_config = config.get_database_by_name("broker")

        return cls(**(kwargs | database_config | queue_config))

    async def _setup(self) -> None:
        await super()._setup()
        await self._create_table()
        await self._start_run()

    async def _destroy(self) -> None:
        await self._stop_run()
        await self._flush_queue()
        await super()._destroy()

    async def _create_table(self) -> None:
        await self.submit_query(self._query_factory.build_create_table(), lock=self._query_factory.build_table_name())

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
            await self.submit_query(self._query_factory.build_update_not_processed(), (entry.id_,))
            self._queue.task_done()

    async def _enqueue(self, message: BrokerMessage) -> None:
        await self.submit_query_and_fetchone(self._query_factory.build_insert(), (message.topic, message.avro_bytes))
        await self._notify_enqueued(message)

    async def _notify_enqueued(self, message: BrokerMessage) -> None:
        await self.submit_query(self._query_factory.build_notify())

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
                    await self.submit_query(self._query_factory.build_update_not_processed(), (entry.id_,))
                    continue

                await self.submit_query(self._query_factory.build_delete_processed(), (entry.id_,))
                return message
            finally:
                self._queue.task_done()

    async def _run(self, max_wait: Optional[float] = 60.0) -> NoReturn:
        async with self.cursor() as cursor:
            await self._listen_entries(cursor)
            try:
                while self._run_task is not None:
                    await self._wait_for_entries(cursor, max_wait)
                    await self._dequeue_batch(cursor)
            finally:
                await self._unlisten_entries(cursor)

    async def _listen_entries(self, cursor: Cursor) -> None:
        # noinspection PyTypeChecker
        await cursor.execute(self._query_factory.build_listen())

    async def _unlisten_entries(self, cursor: Cursor) -> None:
        if not cursor.closed:
            # noinspection PyTypeChecker
            await cursor.execute(self._query_factory.build_unlisten())

    async def _wait_for_entries(self, cursor: Cursor, max_wait: Optional[float]) -> None:
        while True:
            if await self._get_count(cursor):
                return

            with suppress(TimeoutError):
                return await wait_for(consume_queue(cursor.connection.notifies, self._records), max_wait)

    async def _get_count(self, cursor: Cursor) -> int:
        # noinspection PyTypeChecker
        await cursor.execute(self._query_factory.build_count_not_processed(), (self._retry,))
        count = (await cursor.fetchone())[0]
        return count

    async def _dequeue_batch(self, cursor: Cursor) -> None:
        async with cursor.begin():
            rows = await self._dequeue_rows(cursor)

            if not len(rows):
                return

            entries = [_Entry(*row) for row in rows]

            # noinspection PyTypeChecker
            await cursor.execute(self._query_factory.build_mark_processing(), (tuple(entry.id_ for entry in entries),))

            for entry in entries:
                await self._queue.put(entry)

    async def _dequeue_rows(self, cursor: Cursor) -> list[Any]:
        # noinspection PyTypeChecker
        await cursor.execute(self._query_factory.build_select_not_processed(), (self._retry, self._records))
        return await cursor.fetchall()


class PostgreSqlBrokerQueueQueryFactory(ABC):
    """PostgreSql Broker Queue Query Factory class."""

    @abstractmethod
    def build_table_name(self) -> str:
        """Get the table name.

        :return: A ``str`` value.
        """
        raise NotImplementedError

    def build_create_table(self) -> SQL:
        """Build the "create table" query.

        :return: A ``SQL`` instance.
        """
        return SQL(
            f"CREATE TABLE IF NOT EXISTS {self.build_table_name()} ("
            "id BIGSERIAL NOT NULL PRIMARY KEY, "
            "topic VARCHAR(255) NOT NULL, "
            "data BYTEA NOT NULL, "
            "retry INTEGER NOT NULL DEFAULT 0, "
            "processing BOOL NOT NULL DEFAULT FALSE, "
            "created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), "
            "updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW())"
        )

    def build_update_not_processed(self) -> SQL:
        """Build the "update not processed" query.

        :return: A ``SQL`` instance.
        """
        return SQL(
            f"UPDATE {self.build_table_name()} "
            "SET processing = FALSE, retry = retry + 1, updated_at = NOW() WHERE id = %s"
        )

    def build_delete_processed(self) -> SQL:
        """Build the "delete processed" query.

        :return: A ``SQL`` instance.
        """
        return SQL(f"DELETE FROM {self.build_table_name()} WHERE id = %s")

    def build_mark_processing(self) -> SQL:
        """

        :return: A ``SQL`` instance.
        """
        return SQL(f"UPDATE {self.build_table_name()} SET processing = TRUE WHERE id IN %s")

    def build_notify(self) -> SQL:
        """Build the "notify" query.

        :return: A ``SQL`` instance.
        """
        return SQL(f"NOTIFY {self.build_table_name()}")

    def build_listen(self) -> SQL:
        """Build the "listen" query.

        :return: A ``SQL`` instance.
        """
        return SQL(f"LISTEN {self.build_table_name()}")

    def build_unlisten(self) -> SQL:
        """Build the "unlisten" query.

        :return: A ``SQL`` instance.
        """
        return SQL(f"UNLISTEN {self.build_table_name()}")

    def build_count_not_processed(self) -> SQL:
        """Build the "count not processed" query.

        :return:
        """
        return SQL(
            f"SELECT COUNT(*) FROM (SELECT id FROM {self.build_table_name()} "
            "WHERE NOT processing AND retry < %s FOR UPDATE SKIP LOCKED) s"
        )

    def build_insert(self) -> SQL:
        """Build the "insert" query.

        :return: A ``SQL`` instance.
        """
        return SQL(f"INSERT INTO {self.build_table_name()} (topic, data) VALUES (%s, %s) RETURNING id")

    def build_select_not_processed(self) -> SQL:
        """Build the "select not processed" query.

        :return: A ``SQL`` instance.
        """
        return SQL(
            "SELECT id, data "
            f"FROM {self.build_table_name()} "
            "WHERE NOT processing AND retry < %s "
            "ORDER BY created_at "
            "LIMIT %s "
            "FOR UPDATE "
            "SKIP LOCKED"
        )


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


class PostgreSqlBrokerQueueBuilder(Builder):
    """PostgreSql Broker Queue Builder class."""

    def with_config(self, config: Config):
        """Set config.

        :param config: The config to be set.
        :return: This method return the builder instance.
        """
        self.kwargs |= config.get_database_by_name("broker")
        self.kwargs |= config.get_interface_by_name("broker").get("common", dict()).get("queue", dict())
        return super().with_config(config)


PostgreSqlBrokerQueue.set_builder(PostgreSqlBrokerQueueBuilder)
