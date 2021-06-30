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

logger = logging.getLogger(__name__)


class CommandHandler(Handler):
    """Command Handler class."""

    TABLE_NAME = "command_queue"
    ENTRY_MODEL_CLS = Command

    broker: MinosBroker = Provide["command_reply_broker"]

    def __init__(self, broker: MinosBroker = None, **kwargs: Any):
        super().__init__(**kwargs)

        if broker is not None:
            self.broker = broker

    @classmethod
    def _from_config(cls, *args, config: MinosConfig, **kwargs) -> CommandHandler:
        handlers = {item.name: {"controller": item.controller, "action": item.action} for item in config.commands.items}
        return cls(handlers=handlers, **config.commands.queue._asdict(), **kwargs)

    async def dispatch_one(self, row: HandlerEntry) -> NoReturn:
        """Dispatch one row.

        :param row: Row to be dispatched.
        :return: This method does not return anything.
        """
        logger.info(f"Dispatching '{row.data!s}'...")

        command: Command = row.data
        definition_id = command.saga_uuid

        request = CommandRequest(command)
        response = row.callback(request)
        if isawaitable(response):
            response = await response

        if command.reply_topic is not None:
            items = await response.content()
            await self.broker.send(items, topic=command.reply_topic, saga_uuid=definition_id)
