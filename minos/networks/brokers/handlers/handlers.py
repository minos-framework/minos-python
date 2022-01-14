from __future__ import (
    annotations,
)

import logging
from asyncio import (
    CancelledError,
    PriorityQueue,
    Task,
    TimeoutError,
    create_task,
    gather,
    wait_for,
)
from typing import (
    Any,
    KeysView,
    NoReturn,
    Optional,
)

from aiopg import (
    Cursor,
)
from cached_property import (
    cached_property,
)
from dependency_injector.wiring import (
    Provide,
    inject,
)
from psycopg2.sql import (
    SQL,
    Identifier,
)

from minos.common import (
    MinosConfig,
)

from ...utils import (
    consume_queue,
)
from .abc import (
    BrokerHandlerSetup,
)
from .dispatchers import (
    BrokerDispatcher,
)
from .entries import (
    BrokerHandlerEntry,
)

logger = logging.getLogger(__name__)


class BrokerHandler(BrokerHandlerSetup):
    """Broker Handler class."""

    __slots__ = "_handlers", "_records", "_retry", "_queue", "_consumers", "_consumer_concurrency"

    _queue: PriorityQueue[BrokerHandlerEntry]
    _consumers: list[Task]

    def __init__(
        self, dispatcher: BrokerDispatcher, records: int, retry: int, consumer_concurrency: int = 15, **kwargs: Any,
    ):
        super().__init__(**kwargs)
        self._dispatcher = dispatcher

        self._records = records
        self._retry = retry

        self._queue = PriorityQueue(maxsize=self._records)
        self._consumers = list()
        self._consumer_concurrency = consumer_concurrency

    @classmethod
    def _from_config(cls, config: MinosConfig, **kwargs) -> BrokerHandler:
        kwargs["dispatcher"] = cls._get_dispatcher(config, **kwargs)
        # noinspection PyProtectedMember
        return cls(**config.broker.queue._asdict(), **kwargs)

    # noinspection PyUnusedLocal
    @staticmethod
    @inject
    def _get_dispatcher(
        config: MinosConfig,
        dispatcher: Optional[BrokerDispatcher] = None,
        broker_dispatcher: BrokerDispatcher = Provide["broker_dispatcher"],
        **kwargs,
    ) -> BrokerDispatcher:
        if dispatcher is None:
            dispatcher = broker_dispatcher
        if dispatcher is None or isinstance(dispatcher, Provide):
            dispatcher = BrokerDispatcher.from_config(config, **kwargs)
        return dispatcher

    async def _setup(self) -> None:
        await super()._setup()
        await self._create_consumers()

    async def _destroy(self) -> None:
        await self._destroy_consumers()
        await super()._destroy()

    async def _create_consumers(self):
        while len(self._consumers) < self._consumer_concurrency:
            self._consumers.append(create_task(self._consume()))

    async def _destroy_consumers(self):
        for consumer in self._consumers:
            consumer.cancel()
        await gather(*self._consumers, return_exceptions=True)
        self._consumers = list()

        while not self._queue.empty():
            entry = self._queue.get_nowait()
            await self.submit_query(self._queries["update_not_processed"], (entry.id,))

    async def _consume(self) -> None:
        while True:
            await self._consume_one()

    async def _consume_one(self) -> None:
        entry = await self._queue.get()
        try:
            try:
                await self._dispatcher.dispatch(entry.data)
            except (CancelledError, Exception) as exc:
                await self.submit_query(self._queries["update_not_processed"], (entry.id,))
                raise exc
            await self.submit_query(self._queries["delete_processed"], (entry.id,))
        finally:
            self._queue.task_done()

    @property
    def dispatcher(self) -> BrokerDispatcher:
        """Get the dispatcher.

        :return: A ``BrokerDispatcher`` instance.
        """
        return self._dispatcher

    @property
    def consumers(self) -> list[Task]:
        """Get the consumers.

        :return: A list of ``Task`` instances.
        """
        return self._consumers

    @property
    def topics(self) -> KeysView[str]:
        """Get an iterable containing the topic names.

        :return: An ``Iterable`` of ``str``.
        """
        return self._dispatcher.actions.keys()

    async def dispatch_forever(self, max_wait: Optional[float] = 60.0) -> NoReturn:
        """Dispatch the items in the consuming queue forever.

        :param max_wait: Maximum seconds to wait for notifications. If ``None`` the wait is performed until infinity.
        :return: This method does not return anything.
        """
        async with self.cursor() as cursor:
            await self._listen_entries(cursor)
            try:
                while True:
                    await self._wait_for_entries(cursor, max_wait)
                    await self.dispatch(cursor, background_mode=True)
            finally:
                await self._unlisten_entries(cursor)

    async def _listen_entries(self, cursor: Cursor):
        for topic in self.topics:
            # noinspection PyTypeChecker
            await cursor.execute(_LISTEN_QUERY.format(Identifier(topic)))

    async def _unlisten_entries(self, cursor: Cursor) -> None:
        for topic in self.topics:
            # noinspection PyTypeChecker
            await cursor.execute(_UNLISTEN_QUERY.format(Identifier(topic)))

    async def _wait_for_entries(self, cursor: Cursor, max_wait: Optional[float]) -> None:
        if await self._get_count(cursor):
            return

        while True:
            try:
                return await wait_for(consume_queue(cursor.connection.notifies, self._records), max_wait)
            except TimeoutError:
                if await self._get_count(cursor):
                    return

    async def _get_count(self, cursor) -> int:
        if not len(self.topics):
            return 0
        await cursor.execute(_COUNT_NOT_PROCESSED_QUERY, (self._retry, tuple(self.topics)))
        count = (await cursor.fetchone())[0]
        return count

    async def dispatch(self, cursor: Optional[Cursor] = None, background_mode: bool = False) -> None:
        """Dispatch a batch of ``HandlerEntry`` instances from the database's queue.

        :param cursor: The cursor to interact with the database. If ``None`` is provided a new one is acquired.
        :param background_mode: If ``True`` the entries dispatching waits until every entry is processed. Otherwise,
            the dispatching is performed on background.
        :return: This method does not return anything.
        """

        is_external_cursor = cursor is not None
        if not is_external_cursor:
            cursor = await self.cursor().__aenter__()

        async with cursor.begin():
            await cursor.execute(
                self._queries["select_not_processed"], (self._retry, tuple(self.topics), self._records)
            )
            result = await cursor.fetchall()

            if len(result):
                entries = [BrokerHandlerEntry(*row) for row in result]

                await cursor.execute(self._queries["mark_processing"], (tuple(e.id for e in entries),))

                for entry in entries:
                    await self._queue.put(entry)

        if not is_external_cursor:
            await cursor.__aexit__(None, None, None)

        if not background_mode:
            await self._queue.join()

    @cached_property
    def _queries(self) -> dict[str, str]:
        # noinspection PyTypeChecker
        return {
            "count_not_processed": _COUNT_NOT_PROCESSED_QUERY,
            "select_not_processed": _SELECT_NOT_PROCESSED_QUERY,
            "mark_processing": _MARK_PROCESSING_QUERY,
            "delete_processed": _DELETE_PROCESSED_QUERY,
            "update_not_processed": _UPDATE_NOT_PROCESSED_QUERY,
        }


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
