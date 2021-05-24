# Copyright (C) 2020 Clariteia SL
#
# This file is part of minos framework.
#
# Minos framework can not be copied and/or distributed without the express
# permission of Clariteia SL.
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

    TABLE = "command_reply_queue"

    saga_manager: MinosSagaManager = Provide["saga_manager"]

    def __init__(self, *, config: MinosConfig, saga_manager: MinosSagaManager = None, **kwargs: Any):
        super().__init__(table_name=self.TABLE, config=config.saga, **kwargs)

        self._broker_group_name = f"event_{config.service.name}"

        if saga_manager is not None:
            self.saga_manager = saga_manager

    def _build_data(self, value: bytes) -> CommandReply:
        return CommandReply.from_avro_bytes(value)

    async def _dispatch_one(self, row: HandlerEntry) -> NoReturn:
        self.saga_manager.run(reply=row.data)
