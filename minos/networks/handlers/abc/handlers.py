# Copyright (C) 2020 Clariteia SL
#
# This file is part of minos framework.
#
# Minos framework can not be copied and/or distributed without the express
# permission of Clariteia SL.

from __future__ import (
    annotations,
)

import logging
from abc import (
    abstractmethod,
)
from datetime import (
    datetime,
)
from typing import (
    Any,
    Callable,
    NoReturn,
    Optional,
    Type,
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

    async def dispatch(self) -> NoReturn:
        """Event Queue Checker and dispatcher.

        It is in charge of querying the database and calling the action according to the topic.

            1. Get periodically 10 records (or as many as defined in config > queue > records).
            2. Instantiate the action (asynchronous) by passing it the model.
            3. If the invoked function terminates successfully, remove the event from the database.

        Raises:
            Exception: An error occurred inserting record.
        """

        async with self.cursor() as cursor:
            # aiopg works in autocommit mode, meaning that you have to use transaction in manual mode.
            # Read more details: https://aiopg.readthedocs.io/en/stable/core.html#transactions.
            await cursor.execute("BEGIN")

            # Select records and lock them FOR UPDATE
            # noinspection PyTypeChecker
            await cursor.execute(
                _SELECT_NON_PROCESSED_ROWS_QUERY.format(Identifier(self.TABLE_NAME)), (self._retry, self._records)
            )

            result = await cursor.fetchall()
            entries = self._build_entries(result)

            for entry in entries:
                await self._dispatch_one(entry)

            for entry in entries:
                if not entry.failed:
                    query = _DELETE_PROCESSED_QUERY.format(Identifier(self.TABLE_NAME))
                else:
                    query = _UPDATE_NON_PROCESSED_QUERY.format(Identifier(self.TABLE_NAME))

                # noinspection PyTypeChecker
                await cursor.execute(query, (entry.id,))
            # Manually commit
            await cursor.execute("COMMIT")

    async def _dispatch_one(self, entry: HandlerEntry) -> NoReturn:
        logger.debug(f"Dispatching '{entry.data!s}'...")
        if entry.failed:
            return

        try:
            await self.dispatch_one(entry)
        except Exception as exc:
            logger.warning(f"Raised an exception while dispatching {entry.data!s}: {exc!r}")
            entry.failed = True

    def _build_entries(self, rows: list[tuple]) -> list[HandlerEntry]:
        return [self._build_entry(row) for row in rows]

    def _build_entry(self, row: tuple[int, str, int, bytes, int, datetime]) -> HandlerEntry:
        return HandlerEntry.from_raw(row, callback_lookup=self.get_action, data_cls=self.ENTRY_MODEL_CLS)

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

    @abstractmethod
    async def dispatch_one(self, entry: HandlerEntry) -> NoReturn:
        """Dispatch one row.

        :param entry: Entry to be dispatched.
        :return: This method does not return anything.
        """
        raise NotImplementedError


_SELECT_NON_PROCESSED_ROWS_QUERY = SQL(
    "SELECT * FROM {} WHERE retry < %s ORDER BY creation_date LIMIT %s FOR UPDATE SKIP LOCKED"
)

_DELETE_PROCESSED_QUERY = SQL("DELETE FROM {} " "WHERE id = %s")

_UPDATE_NON_PROCESSED_QUERY = SQL("UPDATE {} " "SET retry = retry + 1 " "WHERE id = %s")
