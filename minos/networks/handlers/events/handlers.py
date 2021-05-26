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
    Any,
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

    def __init__(self, *, service_name: str, **kwargs: Any):
        super().__init__(broker_group_name=f"event_{service_name}", **kwargs)

    @classmethod
    def _from_config(cls, *args, config: MinosConfig, **kwargs) -> EventHandler:
        handlers = {item.name: {"controller": item.controller, "action": item.action} for item in config.events.items}
        return cls(service_name=config.service.name, handlers=handlers, **config.events.queue._asdict(), **kwargs)

    def _build_data(self, value: bytes) -> Event:
        return Event.from_avro_bytes(value)

    async def _dispatch_one(self, row: HandlerEntry) -> NoReturn:
        await row.callback(row.topic, row.data)
