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


class CommandHandler(Handler):
    """Command Handler class."""

    TABLE = "command_queue"

    broker: MinosBroker = Provide["command_reply_broker"]

    def __init__(self, *, config: MinosConfig, broker: MinosBroker = None, **kwargs: Any):
        super().__init__(table_name=self.TABLE, config=config.commands, **kwargs)

        self._broker_group_name = f"event_{config.service.name}"

        if broker is not None:
            self.broker = broker

    def _build_data(self, value: bytes) -> Command:
        return Command.from_avro_bytes(value)

    async def _dispatch_one(self, row: HandlerEntry) -> NoReturn:
        command: Command = row.data
        definition_id = command.saga_id
        execution_id = command.task_id

        response = await row.callback(row.topic, command)

        await self.broker.send(response, topic=f"{definition_id}Reply", saga_id=definition_id, task_id=execution_id)
