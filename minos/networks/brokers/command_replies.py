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

    def __init__(self, *args, saga_uuid: Optional[str] = None, **kwargs):
        super().__init__(*args, **kwargs)
        self.saga_uuid = saga_uuid

    @classmethod
    def _from_config(cls, *args, config: MinosConfig, **kwargs) -> CommandReplyBroker:
        return cls(*args, **config.saga.queue._asdict(), **kwargs)

    async def send(
        self, items: list[MinosModel], topic: Optional[str] = None, saga_uuid: Optional[str] = None, **kwargs,
    ) -> int:
        """Send a list of ``Aggregate`` instances.

        :param items: A list of aggregates.
        :param topic: Topic in which the message will be published.
        :param saga_uuid: Saga identifier.
        :return: This method does not return anything.
        """
        if topic is None:
            topic = self.topic
        if saga_uuid is None:
            saga_uuid = self.saga_uuid
        command_reply = CommandReply(topic=f"{topic}Reply", items=items, saga_uuid=saga_uuid)
        return await self.send_bytes(command_reply.topic, command_reply.avro_bytes)
