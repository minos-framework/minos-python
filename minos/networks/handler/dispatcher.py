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
from typing import (
    Any,
    Callable,
    NamedTuple,
    NoReturn,
    Optional,
)

import aiopg

from minos.common import (
    MinosConfig,
    import_module,
)
from minos.common.logs import (
    log,
)

from ..exceptions import (
    MinosNetworkException,
)
from .abc import (
    MinosHandlerSetup,
)


class MinosHandlerDispatcher(MinosHandlerSetup):
    """
    Event Handler

    """

    __slots__ = "_db_dsn", "_handlers", "_event_items", "_topics", "_conf"

    def __init__(self, *, table_name: str, config: NamedTuple, **kwargs: Any):
        super().__init__(table_name=table_name, **kwargs, **config.queue._asdict())
        self._db_dsn = (
            f"dbname={config.queue.database} user={config.queue.user} "
            f"password={config.queue.password} host={config.queue.host}"
        )
        self._handlers = {item.name: {"controller": item.controller, "action": item.action} for item in config.items}
        self._event_items = config.items
        self._topics = list(self._handlers.keys())
        self._conf = config
        self._table_name = table_name

    @classmethod
    def from_config(cls, *args, config: MinosConfig = None, **kwargs) -> Optional[MinosHandlerDispatcher]:
        """Build a new repository from config.
        :param args: Additional positional arguments.
        :param config: Config instance. If `None` is provided, default config is chosen.
        :param kwargs: Additional named arguments.
        :return: A `MinosRepository` instance.
        """
        if config is None:
            config = MinosConfig.get_default()
        if config is None:
            return None
        # noinspection PyProtectedMember
        return cls(*args, config=config, **kwargs)

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

    async def queue_checker(self) -> NoReturn:
        """Event Queue Checker and dispatcher.

        It is in charge of querying the database and calling the action according to the topic.

            1. Get periodically 10 records (or as many as defined in config > queue > records).
            2. Instantiate the action (asynchronous) by passing it the model.
            3. If the invoked function terminates successfully, remove the event from the database.

        Raises:
            Exception: An error occurred inserting record.
        """
        db_dsn = (
            f"dbname={self._conf.queue.database} user={self._conf.queue.user} "
            f"password={self._conf.queue.password} host={self._conf.queue.host}"
        )
        async with aiopg.create_pool(db_dsn) as pool:
            async with pool.acquire() as connect:
                async with connect.cursor() as cur:
                    await cur.execute(
                        "SELECT * FROM %s ORDER BY creation_date ASC LIMIT %d;"
                        % (self._table_name, self._conf.queue.records),
                    )
                    async for row in cur:
                        call_ok = False
                        try:
                            reply_on = self.get_event_handler(topic=row[1])
                            valid_instance, instance = self._is_valid_instance(row[3])
                            if not valid_instance:
                                return
                            await reply_on(row[1], instance)
                            call_ok = True
                        finally:
                            if call_ok:
                                # Delete from database If the event was sent successfully to Kafka.
                                async with connect.cursor() as cur2:
                                    await cur2.execute("DELETE FROM %s WHERE id=%d;" % (self._table_name, row[0]))

    @abstractmethod
    def _is_valid_instance(self, value: bytes):  # pragma: no cover
        raise Exception("Method not implemented")
