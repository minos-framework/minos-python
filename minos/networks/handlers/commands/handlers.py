# Copyright (C) 2020 Clariteia SL
#
# This file is part of minos framework.
#
# Minos framework can not be copied and/or distributed without the express
# permission of Clariteia SL.
from __future__ import (
    annotations,
)

from inspect import (
    isawaitable,
)
from typing import (
    Any,
    NoReturn,
)

from dependency_injector.wiring import (
    Provide,
)

from minos.common import (
    Command,
    MinosBroker,
    MinosConfig,
)

from ..abc import (
    Handler,
)
from ..entries import (
    HandlerEntry,
)
from .messages import (
    CommandRequest,
)


class CommandHandler(Handler):
    """Command Handler class."""

    TABLE_NAME = "command_queue"

    broker: MinosBroker = Provide["command_reply_broker"]

    def __init__(self, *, service_name: str, broker: MinosBroker = None, **kwargs: Any):
        super().__init__(broker_group_name=f"command_{service_name}", **kwargs)

        if broker is not None:
            self.broker = broker

    def _build_data(self, value: bytes) -> Command:
        return Command.from_avro_bytes(value)

    @classmethod
    def _from_config(cls, *args, config: MinosConfig, **kwargs) -> CommandHandler:
        handlers = {item.name: {"controller": item.controller, "action": item.action} for item in config.commands.items}
        return cls(service_name=config.service.name, handlers=handlers, **config.commands.queue._asdict(), **kwargs)

    async def _dispatch_one(self, row: HandlerEntry) -> NoReturn:
        command: Command = row.data
        definition_id = command.saga_uuid

        request = CommandRequest(command)
        response = row.callback(request)
        if isawaitable(response):
            response = await response

        if command.reply_topic is not None:
            items = await response.content()
            await self.broker.send(items, topic=command.reply_topic, saga_uuid=definition_id)
