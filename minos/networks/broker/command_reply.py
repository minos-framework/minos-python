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
    CommandReply,
    MinosConfig,
    MinosConfigException,
)

from .abc import (
    MinosBroker,
)


class MinosCommandReplyBroker(MinosBroker):
    """Minos Command Broker Class."""

    ACTION = "commandReply"

    def __init__(self, *args, saga_id: str, task_id: str, **kwargs):
        super().__init__(*args, **kwargs)
        self.saga_id = saga_id
        self.task_id = task_id

    @classmethod
    def from_config(cls, *args, config: MinosConfig = None, **kwargs) -> Optional[MinosCommandReplyBroker]:
        """Build a new repository from config.
        :param args: Additional positional arguments.
        :param config: Config instance. If `None` is provided, default config is chosen.
        :param kwargs: Additional named arguments.
        :return: A `MinosRepository` instance.
        """
        if config is None:
            config = MinosConfig.get_default()
        if config is None:
            raise MinosConfigException("The config object must be setup.")
        # noinspection PyProtectedMember
        return cls(*args, **config.commands.queue._asdict(), **kwargs)

    async def send(self, items: list[Aggregate]) -> int:
        """Send a list of ``Aggregate`` instances.

        :param items: A list of aggregates.
        :return: This method does not return anything.
        """
        command_reply = CommandReply(topic=self.topic, items=items, saga_id=self.saga_id, task_id=self.task_id)
        return await self._send_bytes(command_reply.topic, command_reply.avro_bytes)
