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

from minos.common import (
    CommandReply,
    MinosConfig,
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

    def __init__(self, *, config: MinosConfig, **kwargs: Any):
        super().__init__(table_name=self.TABLE, config=config.saga, **kwargs)
        self._broker_group_name = f"event_{config.service.name}"

    def _build_data(self, value: bytes) -> CommandReply:
        return CommandReply.from_avro_bytes(value)

    async def _dispatch_one(self, row: HandlerEntry) -> NoReturn:
        await row.callback(row.topic, row.data)
