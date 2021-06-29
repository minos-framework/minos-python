# Copyright (C) 2020 Clariteia SL
#
# This file is part of minos framework.
#
# Minos framework can not be copied and/or distributed without the express
# permission of Clariteia SL.
from __future__ import (
    annotations,
)

from typing import (
    NoReturn,
)

from minos.common import (
    Event,
    MinosConfig,
)

from ..abc import (
    Handler,
)
from ..entries import (
    HandlerEntry,
)


class EventHandler(Handler):
    """Event Handler class."""

    TABLE_NAME = "event_queue"
    ENTRY_MODEL_CLS = Event

    @classmethod
    def _from_config(cls, *args, config: MinosConfig, **kwargs) -> EventHandler:
        handlers = {item.name: {"controller": item.controller, "action": item.action} for item in config.events.items}
        return cls(handlers=handlers, **config.events.queue._asdict(), **kwargs)

    async def dispatch_one(self, row: HandlerEntry) -> NoReturn:
        """Dispatch one row.

        :param row: Row to be dispatched.
        :return: This method does not return anything.
        """
        await row.callback(row.topic, row.data)
