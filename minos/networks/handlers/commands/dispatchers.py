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
    Command,
    MinosConfig,
)

from ..abc import (
    Handler,
)
from ..entries import (
    HandlerEntry,
)


class CommandHandler(Handler):
    """Command Handler class."""

    TABLE = "command_queue"

    def __init__(self, *, config: MinosConfig, **kwargs: Any):
        super().__init__(table_name=self.TABLE, config=config.commands, **kwargs)
        self._broker_group_name = f"event_{config.service.name}"

    def _build_data(self, value: bytes) -> Command:
        return Command.from_avro_bytes(value)

    async def _dispatch_one(self, row: HandlerEntry) -> NoReturn:
        await row.callback(row.topic, row.data)
