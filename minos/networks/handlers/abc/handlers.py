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

    async def dispatch_forever(self, max_wait: Optional[float] = 15.0) -> NoReturn:
        """Dispatch the items in the consuming queue forever.

        :param max_wait: Maximum seconds to wait for notifications. If ``None`` the wait is performed until infinity.
        :return: This method does not return anything.
        """
        async with self.cursor() as cursor:
            await self.dispatch(cursor)

            await cursor.execute(self._queries["listen"])
            try:
                while True:
                    try:
                        await wait_for(consume_queue(cursor.connection.notifies, self._records), max_wait)
                    except TimeoutError:
                        pass
                    finally:
                        await self.dispatch(cursor)
            finally:
                await cursor.execute(self._queries["unlisten"])

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
            await cursor.execute(self._queries["select_non_processed"], (self._retry, self._records))

            result = await cursor.fetchall()
            entries = self._build_entries(result)
            await self._dispatch_entries(entries)

            for entry in entries:
                query_id = "delete_processed" if entry.success else "update_non_processed"
                await cursor.execute(self._queries[query_id], (entry.id,))

        if not is_external_cursor:
            await cursor.__aexit__(None, None, None)

    @cached_property
    def _queries(self) -> dict[str, str]:
        # noinspection PyTypeChecker
        return {
            "listen": _LISTEN_QUERY.format(Identifier(self.TABLE_NAME)),
            "unlisten": _UNLISTEN_QUERY.format(Identifier(self.TABLE_NAME)),
            "select_non_processed": _SELECT_NON_PROCESSED_ROWS_QUERY.format(Identifier(self.TABLE_NAME)),
            "delete_processed": _DELETE_PROCESSED_QUERY.format(Identifier(self.TABLE_NAME)),
            "update_non_processed": _UPDATE_NON_PROCESSED_QUERY.format(Identifier(self.TABLE_NAME)),
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


_SELECT_NON_PROCESSED_ROWS_QUERY = SQL(
    "SELECT * FROM {} WHERE retry < %s ORDER BY creation_date LIMIT %s FOR UPDATE SKIP LOCKED"
)

_DELETE_PROCESSED_QUERY = SQL("DELETE FROM {} WHERE id = %s")

_UPDATE_NON_PROCESSED_QUERY = SQL("UPDATE {} SET retry = retry + 1 WHERE id = %s")

_LISTEN_QUERY = SQL("LISTEN {}")

_UNLISTEN_QUERY = SQL("UNLISTEN {}")
