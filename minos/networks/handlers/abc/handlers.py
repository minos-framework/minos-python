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
from inspect import (
    isclass,
)
from typing import (
    Any,
    Callable,
    NoReturn,
    Type,
)

from psycopg2.sql import (
    SQL,
    Identifier,
)

from minos.common import (
    MinosModel,
    import_module,
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

    def __init__(self, records: int, handlers: dict[str, dict[str, Any]], retry: int, **kwargs: Any):
        super().__init__(**kwargs)
        self._handlers = handlers
        self._records = records
        self._retry = retry

    async def dispatch(self) -> NoReturn:
        """Event Queue Checker and dispatcher.

        It is in charge of querying the database and calling the action according to the topic.

            1. Get periodically 10 records (or as many as defined in config > queue > records).
            2. Instantiate the action (asynchronous) by passing it the model.
            3. If the invoked function terminates successfully, remove the event from the database.

        Raises:
            Exception: An error occurred inserting record.
        """

        pool = await self.pool
        with await pool.cursor() as cursor:
            # aiopg works in autocommit mode, meaning that you have to use transaction in manual mode.
            # Read more details: https://aiopg.readthedocs.io/en/stable/core.html#transactions.
            await cursor.execute("BEGIN")

            # Select records and lock them FOR UPDATE
            await cursor.execute(
                _SELECT_NON_PROCESSED_ROWS_QUERY.format(Identifier(self.TABLE_NAME)), (self._retry, self._records)
            )
            result = await cursor.fetchall()

            for row in result:
                dispatched = await self._dispatch_one(row)
                if dispatched:
                    await cursor.execute(_DELETE_PROCESSED_QUERY.format(Identifier(self.TABLE_NAME)), (row[0],))
                else:
                    await cursor.execute(_UPDATE_NON_PROCESSED_QUERY.format(Identifier(self.TABLE_NAME)), (row[0],))

            # Manually commit
            await cursor.execute("COMMIT")

    async def _dispatch_one(self, row: tuple) -> bool:
        try:
            entry = await self._build_entry(row)
        except Exception as exc:
            logger.warning(f"Raised an exception while building the message with id={row[0]}: {exc!r}")
            return False

        logger.debug(f"Dispatching '{entry.data!s}'...")

        try:
            await self.dispatch_one(entry)
        except Exception as exc:
            logger.warning(f"Raised an exception while dispatching {entry.data!s}: {exc!r}")
            return False

        return True

    async def _build_entry(self, row: tuple[int, str, int, bytes, datetime]) -> HandlerEntry:
        id = row[0]
        topic = row[1]
        callback = self.get_action(row[1])
        partition_id = row[2]
        data = self.ENTRY_MODEL_CLS.from_avro_bytes(row[3])
        retry = row[4]
        created_at = row[5]

        entry = HandlerEntry(id, topic, callback, partition_id, data, retry, created_at)
        return entry

    def get_action(self, topic: str) -> Callable:
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

        event = self._handlers[topic]

        controller = import_module(event["controller"])
        if isclass(controller):
            controller = controller()
        action = getattr(controller, event["action"])

        logger.debug(f"Loaded {action!r} action!")
        return action

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
