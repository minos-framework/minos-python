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
from inspect import (
    isawaitable,
)
from typing import (
    Awaitable,
    Callable,
    NoReturn,
    Union,
)

from minos.common import (
    Event,
    MinosConfig,
    MinosException,
)

from ...decorators import (
    EnrouteBuilder,
)
from ..abc import (
    Handler,
)
from ..entries import (
    HandlerEntry,
)
from ..messages import (
    HandlerRequest,
    ResponseException,
)

logger = logging.getLogger(__name__)


class EventHandler(Handler):
    """Event Handler class."""

    TABLE_NAME = "event_queue"
    ENTRY_MODEL_CLS = Event

    @classmethod
    def _from_config(cls, *args, config: MinosConfig, **kwargs) -> EventHandler:
        decorators = EnrouteBuilder(config.events.service).get_broker_event()

        handlers = {decorator.topic: fn for decorator, fn in decorators.items()}

        return cls(handlers=handlers, **config.broker.queue._asdict(), **kwargs)

    async def dispatch_one(self, entry: HandlerEntry) -> NoReturn:
        """Dispatch one row.

        :param entry: Entry to be dispatched.
        :return: This method does not return anything.
        """
        event = entry.data
        logger.info(f"Dispatching '{event!s}'...")

        fn = self.get_callback(entry.callback)
        await fn(event)

    @staticmethod
    def get_callback(
        fn: Callable[[HandlerRequest], Union[NoReturn, Awaitable[NoReturn]]]
    ) -> Callable[[Event], Awaitable[NoReturn]]:
        """Get the handler function to be used by the Event Handler.

        :param fn: The action function.
        :return: A wrapper function around the given one that is compatible with the Event Handler API.
        """

        async def _fn(event: Event) -> NoReturn:
            try:
                request = HandlerRequest(event)
                response = fn(request)
                if isawaitable(response):
                    await response
            except ResponseException as exc:
                logger.info(f"Raised a user exception: {exc!s}")
            except MinosException as exc:
                logger.warning(f"Raised a 'minos' exception: {exc!r}")
            except Exception as exc:
                logger.exception(f"Raised an exception: {exc!r}.")

        return _fn
