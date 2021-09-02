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
from asyncio import (
    gather,
)
from collections import (
    defaultdict,
)
from inspect import (
    isawaitable,
)
from operator import (
    attrgetter,
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

uuid_getter = attrgetter("data.data.uuid")
version_getter = attrgetter("data.data.version")


class EventHandler(Handler):
    """Event Handler class."""

    ENTRY_MODEL_CLS = Event

    @classmethod
    def _from_config(cls, *args, config: MinosConfig, **kwargs) -> EventHandler:
        decorators = EnrouteBuilder(config.events.service, config).get_broker_event()

        handlers = {decorator.topic: fn for decorator, fn in decorators.items()}

        return cls(handlers=handlers, **config.broker.queue._asdict(), **kwargs)

    async def _dispatch_entries(self, entries: list[HandlerEntry[Event]]) -> NoReturn:
        grouped = defaultdict(list)
        for entry in entries:
            grouped[uuid_getter(entry)].append(entry)

        for group in grouped.values():
            group.sort(key=version_getter)

        futures = (self._dispatch_group(group) for group in grouped.values())
        await gather(*futures)

    async def _dispatch_group(self, entries: list[HandlerEntry[Event]]):
        for entry in entries:
            await self._dispatch_one(entry)

    async def dispatch_one(self, entry: HandlerEntry[Event]) -> NoReturn:
        """Dispatch one row.

        :param entry: Entry to be dispatched.
        :return: This method does not return anything.
        """
        logger.info(f"Dispatching '{entry!s}'...")

        fn = self.get_callback(entry.callback)
        await fn(entry.data)

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
