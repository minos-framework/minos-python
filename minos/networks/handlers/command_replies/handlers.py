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
    Any,
    NoReturn,
)

from dependency_injector.wiring import (
    Provide,
)

from minos.common import (
    CommandReply,
    MinosConfig,
    MinosSagaManager,
)

from ..abc import (
    Handler,
)
from ..entries import (
    HandlerEntry,
)

logger = logging.getLogger(__name__)


class CommandReplyHandler(Handler):
    """Command Reply Handler class."""

    TABLE_NAME = "command_reply_queue"
    ENTRY_MODEL_CLS = CommandReply

    saga_manager: MinosSagaManager = Provide["saga_manager"]

    def __init__(self, saga_manager: MinosSagaManager = None, **kwargs: Any):
        super().__init__(**kwargs)

        if saga_manager is not None:
            self.saga_manager = saga_manager

    @classmethod
    def _from_config(cls, *args, config: MinosConfig, **kwargs) -> CommandReplyHandler:
        handlers = {
            f"{item.name}Reply": {"controller": item.controller, "action": item.action} for item in config.saga.items
        }
        return cls(*args, handlers=handlers, **config.saga.queue._asdict(), **kwargs)

    async def dispatch_one(self, entry: HandlerEntry) -> NoReturn:
        """Dispatch one row.

        :param entry: Entry to be dispatched.
        :return: This method does not return anything.
        """
        logger.info(f"Dispatching '{entry.data!s}'...")
        await self.saga_manager.run(reply=entry.data)
