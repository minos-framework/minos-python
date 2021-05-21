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
    CommandReply,
    MinosConfig,
    MinosModel,
)

from .abc import (
    Broker,
)


class CommandReplyBroker(Broker):
    """Minos Command Broker Class."""

    ACTION = "commandReply"

    def __init__(self, *args, saga_id: str, task_id: str, **kwargs):
        super().__init__(*args, **kwargs)
        self.saga_id = saga_id
        self.task_id = task_id

    @classmethod
    def _from_config(cls, *args, config: MinosConfig, **kwargs) -> CommandReplyBroker:
        return cls(*args, **config.saga.queue._asdict(), **kwargs)

    async def send(
        self,
        items: list[MinosModel],
        topic: Optional[str] = None,
        saga_id: Optional[str] = None,
        task_id: Optional[str] = None,
        **kwargs
    ) -> int:
        """Send a list of ``Aggregate`` instances.

        :param items: A list of aggregates.
        :param topic: Topic in which the message will be published.
        :param saga_id: Saga identifier.
        :param task_id: Saga execution identifier.
        :return: This method does not return anything.
        """
        if topic is None:
            topic = self.topic
        if saga_id is None:
            saga_id = self.saga_id
        if task_id is None:
            task_id = self.task_id
        command_reply = CommandReply(topic=topic, items=items, saga_id=saga_id, task_id=task_id)
        return await self._send_bytes(command_reply.topic, command_reply.avro_bytes)
