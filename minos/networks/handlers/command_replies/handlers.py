"""minos.networks.handlers.command_replies.handlers module."""

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

    ENTRY_MODEL_CLS = CommandReply

    saga_manager: MinosSagaManager = Provide["saga_manager"]

    def __init__(self, saga_manager: MinosSagaManager = None, **kwargs: Any):
        super().__init__(**kwargs)

        if saga_manager is not None:
            self.saga_manager = saga_manager

    @classmethod
    def _from_config(cls, *args, config: MinosConfig, **kwargs) -> CommandReplyHandler:
        handlers = {f"{config.service.name}Reply": None}
        return cls(*args, handlers=handlers, **config.broker.queue._asdict(), **kwargs)

    async def dispatch_one(self, entry: HandlerEntry[CommandReply]) -> NoReturn:
        """Dispatch one row.

        :param entry: Entry to be dispatched.
        :return: This method does not return anything.
        """
        logger.info(f"Dispatching '{entry!s}'...")
        await self.saga_manager.run(reply=entry.data, pause_on_disk=True, raise_on_error=False, return_execution=False)
