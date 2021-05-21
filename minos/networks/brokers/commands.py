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
    Command,
    MinosConfig,
    MinosModel,
)

from .abc import (
    Broker,
)


class CommandBroker(Broker):
    """Minos Command Broker Class."""

    ACTION = "command"

    def __init__(self, *args, saga_id: str, task_id: str, reply_on: str, **kwargs):
        super().__init__(*args, **kwargs)
        self.reply_on = reply_on
        self.saga_id = saga_id
        self.task_id = task_id

    @classmethod
    def _from_config(cls, *args, config: MinosConfig, **kwargs) -> CommandBroker:
        return cls(*args, **config.commands.queue._asdict(), **kwargs)

    async def send(
        self,
        items: list[MinosModel],
        topic: Optional[str] = None,
        saga_id: Optional[str] = None,
        task_id: Optional[str] = None,
        reply_on: Optional[str] = None,
        **kwargs
    ) -> int:
        """Send a list of ``Aggregate`` instances.

        :param items: A list of aggregates.
        :param topic: Topic in which the message will be published.
        :param saga_id: Saga identifier.
        :param task_id: Saga execution identifier.
        :param reply_on: Topic name in which the reply will be published.
        :return: This method does not return anything.
        """
        if topic is None:
            topic = self.topic
        if saga_id is None:
            saga_id = self.saga_id
        if task_id is None:
            task_id = self.task_id
        if reply_on is None:
            reply_on = self.reply_on
        command = Command(topic, items, saga_id, task_id, reply_on)
        return await self._send_bytes(command.topic, command.avro_bytes)
