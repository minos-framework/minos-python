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
    NamedTuple,
    NoReturn,
)

from minos.common import (
    MinosConfig,
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

    __slots__ = "_handlers", "_event_items", "_topics", "_conf"

    def __init__(self, *, table_name: str, config: NamedTuple, **kwargs: Any):
        super().__init__(table_name=table_name, **kwargs, **config.queue._asdict())
        self._handlers = {item.name: {"controller": item.controller, "action": item.action} for item in config.items}
        self._event_items = config.items
        self._topics = list(self._handlers.keys())
        self._conf = config
        self._table_name = table_name

    @classmethod
    def _from_config(cls, *args, config: MinosConfig, **kwargs) -> Handler:
        return cls(*args, config=config, **kwargs)

    async def dispatch(self) -> NoReturn:
        """Event Queue Checker and dispatcher.

        It is in charge of querying the database and calling the action according to the topic.

            1. Get periodically 10 records (or as many as defined in config > queue > records).
            2. Instantiate the action (asynchronous) by passing it the model.
            3. If the invoked function terminates successfully, remove the event from the database.

        Raises:
            Exception: An error occurred inserting record.
        """
        iterable = self.submit_query_and_iter(
            "SELECT * FROM %s ORDER BY creation_date ASC LIMIT %d;" % (self._table_name, self._conf.queue.records),
        )
        async for row in iterable:
            try:
                await self.dispatch_one(row)
            except Exception as exc:
                log.warning(exc)
                continue
            await self.submit_query("DELETE FROM %s WHERE id=%d;" % (self._table_name, row[0]))

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
        created_at = row[4]

        entry = HandlerEntry(id, topic, callback, partition_id, data, created_at)

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
        for event in self._event_items:
            if event.name == topic:
                # the topic exist, get the controller and the action
                controller = event.controller
                action = event.action

                object_class = import_module(controller)
                log.debug(object_class())
                instance_class = object_class()
                class_method = getattr(instance_class, action)

                return class_method
        raise MinosNetworkException(
            f"topic {topic} have no controller/action configured, " f"please review th configuration file"
        )

    @abstractmethod
    def _build_data(self, value: bytes) -> MinosModel:
        raise NotImplementedError

    @abstractmethod
    async def _dispatch_one(self, row: HandlerEntry) -> NoReturn:
        raise NotImplementedError
