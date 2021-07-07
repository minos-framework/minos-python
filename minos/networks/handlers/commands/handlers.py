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
    Awaitable,
    Callable,
    NoReturn,
    Tuple,
    Union,
)

from dependency_injector.wiring import (
    Provide,
)

from minos.common import (
    Command,
    CommandStatus,
    MinosBroker,
    MinosConfig,
    MinosException,
    Model,
    ResponseException,
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

    async def dispatch_one(self, entry: HandlerEntry) -> NoReturn:
        """Dispatch one row.

        :param entry: Entry to be dispatched.
        :return: This method does not return anything.
        """
        command = entry.data
        logger.info(f"Dispatching '{command!s}'...")

        fn = self.get_callback(entry.callback)
        items, status = await fn(command)

        await self.broker.send(items, topic=command.reply_topic, saga_uuid=command.saga_uuid, status=status)

    @staticmethod
    def get_callback(
        fn: Callable[[CommandRequest], Union[CommandRequest, Awaitable[CommandRequest]]]
    ) -> Callable[[Command], Awaitable[Tuple[list[Model], CommandStatus]]]:
        """Get the handler function to be used by the Command Handler.

        :param fn: The action function.
        :return: A wrapper function around the given one that is compatible with the Command Handler API.
        """

        async def _fn(command: Command) -> Tuple[list[Model], CommandStatus]:
            try:
                request = CommandRequest(command)
                response = fn(request)
                if isawaitable(response):
                    response = await response
                return await response.content(), CommandStatus.SUCCESS
            except ResponseException as exc:
                logger.info(f"Raised a user exception: {exc!s}")
                return [], CommandStatus.ERROR
            except MinosException as exc:
                logger.warning(f"Raised a 'minos' exception: {exc!r}")
                return [], CommandStatus.SYSTEM_ERROR
            except Exception as exc:
                logger.exception(f"Raised an exception: {exc!r}.")
                return [], CommandStatus.SYSTEM_ERROR

        return _fn
