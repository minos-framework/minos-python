"""minos.networks.abc.handlers module."""

from __future__ import (
    annotations,
)

import logging
from abc import (
    abstractmethod,
)
from asyncio import (
    TimeoutError,
    gather,
    wait_for,
)
from typing import (
    Any,
    Callable,
    NoReturn,
    Optional,
    Type,
)

from aiopg import (
    Cursor,
)
from cached_property import (
    cached_property,
)
from psycopg2.sql import (
    SQL,
    Identifier,
)

from minos.common import (
    MinosModel,
)

from ...exceptions import (
    MinosActionNotFoundException,
)
from ...utils import (
    consume_queue,
)
from ..entries import (
    HandlerEntry,
)
from .setups import (
    HandlerSetup,
)

logger = logging.getLogger(__name__)


class Handler(HandlerSetup):
    """
    Event Handler

    """

    __slots__ = "_handlers", "_records", "_retry"

    ENTRY_MODEL_CLS: Type[MinosModel]

    def __init__(self, records: int, handlers: dict[str, Optional[Callable]], retry: int, **kwargs: Any):
        super().__init__(**kwargs)
        self._handlers = handlers
        self._records = records
        self._retry = retry

    @property
    def handlers(self) -> dict[str, Optional[Callable]]:
        """Handlers getter.

        :return: A dictionary in which the keys are topics and the values are the handler.
        """
        return self._handlers

    @property
    def topics(self):
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
                    await self.dispatch(cursor)
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
        await cursor.execute(self._queries["count_not_processed"], (self._retry, tuple(self.topics)))
        count = (await cursor.fetchone())[0]
        return count

    async def dispatch(self, cursor: Optional[Cursor] = None) -> None:
        """Event Queue Checker and dispatcher.

        It is in charge of querying the database and calling the action according to the topic.

            1. Get periodically 10 records (or as many as defined in config > queue > records).
            2. Instantiate the action (asynchronous) by passing it the model.
            3. If the invoked function terminates successfully, remove the event from the database.

        Raises:
            Exception: An error occurred inserting record.
        """

        is_external_cursor = cursor is not None
        if not is_external_cursor:
            cursor = await self.cursor().__aenter__()

        async with cursor.begin():
            await cursor.execute(
                self._queries["select_not_processed"], (self._retry, tuple(self.topics), self._records)
            )

            result = await cursor.fetchall()
            entries = self._build_entries(result)
            await self._dispatch_entries(entries)

            for entry in entries:
                query_id = "delete_processed" if entry.success else "update_not_processed"
                await cursor.execute(self._queries[query_id], (entry.id,))

        if not is_external_cursor:
            await cursor.__aexit__(None, None, None)

    @cached_property
    def _queries(self) -> dict[str, str]:
        # noinspection PyTypeChecker
        return {
            "count_not_processed": _COUNT_NOT_PROCESSED_QUERY,
            "select_not_processed": _SELECT_NOT_PROCESSED_QUERY,
            "delete_processed": _DELETE_PROCESSED_QUERY,
            "update_not_processed": _UPDATE_NOT_PROCESSED_QUERY,
        }

    def _build_entries(self, rows: list[tuple]) -> list[HandlerEntry]:
        kwargs = {"callback_lookup": self.get_action, "data_cls": self.ENTRY_MODEL_CLS}
        return [HandlerEntry(*row, **kwargs) for row in rows]

    async def _dispatch_entries(self, entries: list[HandlerEntry]) -> NoReturn:
        futures = (self._dispatch_one(entry) for entry in entries)
        await gather(*futures)

    async def _dispatch_one(self, entry: HandlerEntry) -> NoReturn:
        logger.debug(f"Dispatching '{entry!r}'...")
        try:
            await self.dispatch_one(entry)
        except Exception as exc:
            logger.warning(f"Raised an exception while dispatching {entry!r}: {exc!r}")
            entry.exception = exc

    @abstractmethod
    async def dispatch_one(self, entry: HandlerEntry[ENTRY_MODEL_CLS]) -> NoReturn:
        """Dispatch one row.

        :param entry: Entry to be dispatched.
        :return: This method does not return anything.
        """
        raise NotImplementedError

    def get_action(self, topic: str) -> Optional[Callable]:
        """Get Event instance to call.

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

        if handler is None:
            return

        logger.debug(f"Loaded {handler!r} action!")
        return handler


# noinspection SqlDerivedTableAlias
_COUNT_NOT_PROCESSED_QUERY = SQL(
    "SELECT COUNT(*) FROM (SELECT id FROM consumer_queue WHERE retry < %s AND topic IN %s FOR UPDATE SKIP LOCKED) s"
)

_SELECT_NOT_PROCESSED_QUERY = SQL(
    "SELECT * "
    "FROM consumer_queue "
    "WHERE retry < %s AND topic IN %s "
    "ORDER BY creation_date "
    "LIMIT %s "
    "FOR UPDATE SKIP LOCKED"
)

_DELETE_PROCESSED_QUERY = SQL("DELETE FROM consumer_queue WHERE id = %s")

_UPDATE_NOT_PROCESSED_QUERY = SQL("UPDATE consumer_queue SET retry = retry + 1 WHERE id = %s")

_LISTEN_QUERY = SQL("LISTEN {}")

_UNLISTEN_QUERY = SQL("UNLISTEN {}")
