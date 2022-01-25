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

from minos.common import (
    MinosConfig,
    PostgreSqlMinosDatabase,
)

from ....utils import (
    consume_queue,
)
from ...messages import (
    BrokerMessage,
)
from .abc import (
    BrokerRepository,
)

logger = logging.getLogger(__name__)


class PostgreSqlBrokerRepository(BrokerRepository, PostgreSqlMinosDatabase, ABC):
    """PostgreSql Broker Publisher Repository class."""

    _queue: PriorityQueue[_Entry]

    def __init__(self, *args, retry: int, records: int, **kwargs):
        super().__init__(*args, **kwargs)
        self._retry = retry
        self._records = records

        self._queue = PriorityQueue(maxsize=records)

        self._run_task = None

    @classmethod
    def _from_config(cls, config: MinosConfig, **kwargs) -> PostgreSqlBrokerRepository:
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
        await self.submit_query(self._CREATE_TABLE_QUERY, lock=self._TABLE_NAME)

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

    async def _run(self, max_wait: Optional[float] = 60.0) -> NoReturn:
        async with self.cursor() as cursor:
            await self._listen_entries(cursor)
            try:
                while self._run_task is not None:
                    await self._wait_for_entries(cursor, max_wait)
                    await self._dequeue_batch(cursor)
            finally:
                await self._unlisten_entries(cursor)

    @abstractmethod
    async def _listen_entries(self, cursor: Cursor) -> None:
        raise NotImplementedError

    @abstractmethod
    async def _unlisten_entries(self, cursor: Cursor) -> None:
        raise NotImplementedError

    async def _wait_for_entries(self, cursor: Cursor, max_wait: Optional[float]) -> None:
        while True:
            if await self._get_count(cursor):
                return

            with suppress(TimeoutError):
                return await wait_for(consume_queue(cursor.connection.notifies, self._records), max_wait)

    @abstractmethod
    async def _get_count(self, cursor: Cursor) -> int:
        raise NotImplementedError

    async def _dequeue_batch(self, cursor: Cursor) -> None:
        async with cursor.begin():
            rows = await self._dequeue_rows(cursor)

            if not len(rows):
                return

            entries = [_Entry(*row) for row in rows]

            # noinspection PyTypeChecker
            await cursor.execute(self._MARK_PROCESSING_QUERY, (tuple(entry.id_ for entry in entries),))

            for entry in entries:
                await self._queue.put(entry)

    @abstractmethod
    async def _dequeue_rows(self, cursor: Cursor) -> list[Any]:
        raise NotImplementedError


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
