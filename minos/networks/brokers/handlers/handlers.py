from __future__ import (
    annotations,
)

import logging
from asyncio import (
    PriorityQueue,
    Task,
    TimeoutError,
    create_task,
    gather,
    wait_for,
)
from inspect import (
    isawaitable,
)
from typing import (
    Any,
    Awaitable,
    Callable,
    KeysView,
    NoReturn,
    Optional,
    Union,
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

from ...decorators import (
    EnrouteBuilder,
)
from ...exceptions import (
    MinosActionNotFoundException,
)
from ...requests import (
    USER_CONTEXT_VAR,
    Response,
    ResponseException,
)
from ...utils import (
    consume_queue,
)
from ..messages import (
    BrokerMessage,
    BrokerMessageStatus,
)
from ..publishers import (
    BrokerPublisher,
)
from .abc import (
    BrokerHandlerSetup,
)
from .entries import (
    BrokerHandlerEntry,
)
from .requests import (
    BrokerRequest,
    BrokerResponse,
)

logger = logging.getLogger(__name__)


class BrokerHandler(BrokerHandlerSetup):
    """TODO"""

    __slots__ = "_handlers", "_records", "_retry", "_queue", "_consumers", "_consumer_concurrency"

    @inject
    def __init__(
        self,
        records: int,
        handlers: dict[str, Optional[Callable]],
        retry: int,
        consumer_concurrency: int = 15,
        publisher: BrokerPublisher = Provide["command_reply_broker"],
        **kwargs: Any,
    ):
        super().__init__(**kwargs)
        self._handlers = handlers
        self._records = records
        self._retry = retry

        self._queue = PriorityQueue(maxsize=self._records)
        self._consumers: list[Task] = list()
        self._consumer_concurrency = consumer_concurrency

        self._publisher = publisher

    @property
    def publisher(self) -> BrokerPublisher:
        """TODO"""
        return self._publisher

    @classmethod
    def _from_config(cls, *args, config: MinosConfig, **kwargs) -> BrokerHandler:
        handlers = cls._handlers_from_config(config, **kwargs)
        # noinspection PyProtectedMember
        return cls(handlers=handlers, **config.broker.queue._asdict(), **kwargs)

    @staticmethod
    def _handlers_from_config(
        config: MinosConfig, **kwargs
    ) -> dict[str, Callable[[BrokerRequest], Awaitable[Optional[BrokerResponse]]]]:
        builder = EnrouteBuilder(*config.services, middleware=config.middleware)
        decorators = builder.get_broker_command_query_event(config=config, **kwargs)
        handlers = {decorator.topic: fn for decorator, fn in decorators.items()}
        return handlers

    async def _setup(self) -> None:
        await super()._setup()

        for _ in range(self._consumer_concurrency):
            self._consumers.append(create_task(self._consume()))

    async def _destroy(self) -> None:
        for consumer in self._consumers:
            consumer.cancel()
        await gather(*self._consumers, return_exceptions=True)
        self._consumers = list()

        while not self._queue.empty():
            entry = self._queue.get_nowait()
            await self.submit_query(self._queries["update_not_processed"], (entry.id,))

        await super()._destroy()

    async def _consume(self) -> None:
        while True:
            await self._consume_one()

    async def _consume_one(self) -> None:
        entry = await self._queue.get()
        try:
            await self._dispatch_one(entry)
        finally:
            self._queue.task_done()

    @property
    def handlers(self) -> dict[str, Optional[Callable]]:
        """Handlers getter.

        :return: A dictionary in which the keys are topics and the values are the handler.
        """
        return self._handlers

    @property
    def topics(self) -> KeysView[str]:
        """Get an iterable containing the topic names.

        :return: An ``Iterable`` of ``str``.
        """
        return self.handlers.keys()

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
        """TODO

        :param cursor: TODO
        :param background_mode: TODO
        :return: TODO
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
                entries = self._build_entries(result)

                await cursor.execute(self._queries["mark_processing"], (tuple(e.id for e in entries),))

                for entry in entries:
                    await self._queue.put(entry)

        if not is_external_cursor:
            await cursor.__aexit__(None, None, None)

        if not background_mode:
            await self._queue.join()

    def _build_entries(self, rows: list[tuple]) -> list[BrokerHandlerEntry]:
        kwargs = {"callback_lookup": self.get_action}
        return [BrokerHandlerEntry(*row, **kwargs) for row in rows]

    async def _dispatch_one(self, entry: BrokerHandlerEntry) -> None:
        logger.debug(f"Dispatching '{entry!r}'...")
        try:
            await self.dispatch_one(entry)
        except Exception as exc:
            logger.warning(f"Raised an exception while dispatching {entry!r}: {exc!r}")
            entry.exception = exc
        finally:
            query_id = "delete_processed" if entry.success else "update_not_processed"
            await self.submit_query(self._queries[query_id], (entry.id,))

    async def dispatch_one(self, entry: BrokerHandlerEntry) -> None:
        """Dispatch one row.

        :param entry: Entry to be dispatched.
        :return: This method does not return anything.
        """
        logger.info(f"Dispatching '{entry!s}'...")

        fn = self.get_callback(entry.callback)
        message = entry.data
        items, status = await fn(message)

        if getattr(message, "reply_topic", None) is not None:
            await self.publisher.send(items, topic=message.reply_topic, saga=message.saga, status=status)

    @staticmethod
    def get_callback(
        fn: Callable[[BrokerRequest], Union[Optional[BrokerRequest], Awaitable[Optional[BrokerRequest]]]]
    ) -> Callable[[BrokerMessage], Awaitable[tuple[Any, BrokerMessageStatus]]]:
        """Get the handler function to be used by the Broker Handler.

        :param fn: The action function.
        :return: A wrapper function around the given one that is compatible with the Broker Handler API.
        """

        async def _fn(raw: BrokerMessage) -> tuple[Any, BrokerMessageStatus]:
            request = BrokerRequest(raw)
            token = USER_CONTEXT_VAR.set(request.user)

            try:
                response = fn(request)
                if isawaitable(response):
                    response = await response
                if isinstance(response, Response):
                    response = await response.content()
                return response, BrokerMessageStatus.SUCCESS
            except ResponseException as exc:
                logger.warning(f"Raised an application exception: {exc!s}")
                return repr(exc), BrokerMessageStatus.ERROR
            except Exception as exc:
                logger.exception(f"Raised a system exception: {exc!r}")
                return repr(exc), BrokerMessageStatus.SYSTEM_ERROR
            finally:
                USER_CONTEXT_VAR.reset(token)

        return _fn

    def get_action(self, topic: str) -> Optional[Callable]:
        """Get handling function to be called.

        Gets the instance of the class and method to call.

        Args:
            topic: Kafka topic. Example: "TicketAdded"

        Raises:
            MinosNetworkException: topic TicketAdded have no controller/action configured, please review th
                configuration file.
        """
        if topic not in self._handlers:
            raise MinosActionNotFoundException(
                f"topic {topic} have no controller/action configured, " f"please review th configuration file"
            )

        handler = self._handlers[topic]

        logger.debug(f"Loaded {handler!r} action!")
        return handler

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
