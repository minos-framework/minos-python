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

logger = logging.getLogger(__name__)


class EventHandler(Handler):
    """Event Handler class."""

    TABLE_NAME = "event_queue"
    ENTRY_MODEL_CLS = Event

    @classmethod
    def _from_config(cls, *args, config: MinosConfig, **kwargs) -> EventHandler:
        handlers = {item.name: {"controller": item.controller, "action": item.action} for item in config.events.items}
        return cls(handlers=handlers, **config.events.queue._asdict(), **kwargs)

    async def dispatch_one(self, entry: HandlerEntry) -> NoReturn:
        """Dispatch one row.

        :param entry: Entry to be dispatched.
        :return: This method does not return anything.
        """
        logger.info(f"Dispatching '{entry.data!s}'...")
        await entry.callback(entry.topic, entry.data)
