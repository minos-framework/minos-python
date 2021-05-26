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


class CommandReplyHandler(Handler):
    """Command Reply Handler class."""

    TABLE_NAME = "command_reply_queue"

    saga_manager: MinosSagaManager = Provide["saga_manager"]

    def __init__(self, *, service_name: str, saga_manager: MinosSagaManager = None, **kwargs: Any):
        super().__init__(broker_group_name=f"command_reply_{service_name}", **kwargs)

        if saga_manager is not None:
            self.saga_manager = saga_manager

    @classmethod
    def _from_config(cls, *args, config: MinosConfig, **kwargs) -> CommandReplyHandler:
        handlers = {
            f"{item.name}Reply": {"controller": item.controller, "action": item.action} for item in config.saga.items
        }
        return cls(*args, service_name=config.service.name, handlers=handlers, **config.saga.queue._asdict(), **kwargs,)

    def _build_data(self, value: bytes) -> CommandReply:
        return CommandReply.from_avro_bytes(value)

    async def _dispatch_one(self, row: HandlerEntry) -> NoReturn:
        await self.saga_manager.run(reply=row.data)
