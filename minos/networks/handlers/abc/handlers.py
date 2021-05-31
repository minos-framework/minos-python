# Copyright (C) 2020 Clariteia SL
#
# This file is part of minos framework.
#
# Minos framework can not be copied and/or distributed without the express
# permission of Clariteia SL.

from __future__ import (
    annotations,
)

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
)

from minos.common import (
    MinosModel,
    import_module,
)
from minos.common.logs import (
    log,
)

from ...exceptions import (
    MinosNetworkException,
)
from ..entries import (
    HandlerEntry,
)
from .setups import (
    HandlerSetup,
)


class Handler(HandlerSetup):
    """
    Event Handler

    """

    __slots__ = "_handlers", "_handlers"

    def __init__(self, *, records: int, handlers: dict[str, dict[str, Any]], **kwargs: Any):
        super().__init__(**kwargs)
        self._handlers = handlers
        self._records = records

    async def dispatch(self) -> NoReturn:
        """Event Queue Checker and dispatcher.

        It is in charge of querying the database and calling the action according to the topic.

            1. Get periodically 10 records (or as many as defined in config > queue > records).
            2. Instantiate the action (asynchronous) by passing it the model.
            3. If the invoked function terminates successfully, remove the event from the database.

        Raises:
            Exception: An error occurred inserting record.
        """
        iterable = self.submit_query_and_iter(_SELECT_NON_PROCESSED_ROWS_QUERY % (self.TABLE_NAME, 3, self._records),)
        async for row in iterable:
            dispatched = False
            try:
                await self.dispatch_one(row)
                dispatched = True
            except Exception as exc:
                log.warning(exc)
            finally:
                if dispatched:
                    await self.submit_query(_DELETE_PROCESSED_QUERY % (self.TABLE_NAME, row[0]))
                else:
                    await self.submit_query(_UPDATE_NON_PROCESSED_QUERY % (self.TABLE_NAME, row[0]))

    async def dispatch_one(self, row: tuple[int, str, int, bytes, datetime]) -> NoReturn:
        """Dispatch one row.

        :param row: Row to be dispatched.
        :return: This method does not return anything.
        """
        id = row[0]
        topic = row[1]
        callback = self.get_event_handler(row[1])
        partition_id = row[2]
        data = self._build_data(row[3])
        retry = row[4]
        created_at = row[5]

        entry = HandlerEntry(id, topic, callback, partition_id, data, retry, created_at)

        await self._dispatch_one(entry)

    def get_event_handler(self, topic: str) -> Callable:

        """Get Event instance to call.

        Gets the instance of the class and method to call.

        Args:
            topic: Kafka topic. Example: "TicketAdded"

        Raises:
            MinosNetworkException: topic TicketAdded have no controller/action configured, please review th
                configuration file.
        """
        if topic not in self._handlers:
            raise MinosNetworkException(
                f"topic {topic} have no controller/action configured, " f"please review th configuration file"
            )

        event = self._handlers[topic]
        # the topic exist, get the controller and the action
        controller = event["controller"]
        action = event["action"]

        object_class = import_module(controller)
        log.debug(object_class())
        instance_class = object_class()
        class_method = getattr(instance_class, action)

        return class_method

    @abstractmethod
    def _build_data(self, value: bytes) -> MinosModel:
        raise NotImplementedError

    @abstractmethod
    async def _dispatch_one(self, row: HandlerEntry) -> NoReturn:
        raise NotImplementedError


_SELECT_NON_PROCESSED_ROWS_QUERY = """
SELECT *
FROM %s
WHERE retry <= %d
ORDER BY creation_date
LIMIT %d;
""".strip()

_DELETE_PROCESSED_QUERY = """
DELETE FROM %s
WHERE id = %d;
""".strip()

_UPDATE_NON_PROCESSED_QUERY = """
UPDATE %s
    SET retry = retry + 1
WHERE id = %s;
""".strip()
