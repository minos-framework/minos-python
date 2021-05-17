"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from __future__ import (
    annotations,
)

from typing import (
    Optional,
)

from minos.common import (
    Aggregate,
    Command,
    MinosConfig,
)

from .abc import (
    MinosBroker,
)


class MinosCommandBroker(MinosBroker):
    """Minos Command Broker Class."""

    ACTION = "command"

    def __init__(self, *args, saga_id: str, task_id: str, reply_on: str, **kwargs):
        super().__init__(*args, **kwargs)
        self.reply_on = reply_on
        self.saga_id = saga_id
        self.task_id = task_id

    @classmethod
    def from_config(cls, *args, config: MinosConfig = None, **kwargs) -> Optional[MinosCommandBroker]:
        """Build a new repository from config.
        :param args: Additional positional arguments.
        :param config: Config instance. If `None` is provided, default config is chosen.
        :param kwargs: Additional named arguments.
        :return: A `MinosRepository` instance.
        """
        if config is None:
            config = MinosConfig.get_default()
        if config is None:
            return None
        # noinspection PyProtectedMember
        return cls(*args, **config.commands.queue._asdict(), **kwargs)

    async def send(self, items: list[Aggregate]) -> int:
        """Send a list of ``Aggregate`` instances.

        :param items: A list of aggregates.
        :return: This method does not return anything.
        """
        command = Command(self.topic, items, self.saga_id, self.task_id, self.reply_on)
        return await self._send_bytes(command.topic, command.avro_bytes)
