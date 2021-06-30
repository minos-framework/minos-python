"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from __future__ import (
    annotations,
)

import logging
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

logger = logging.getLogger(__name__)


class CommandBroker(Broker):
    """Minos Command Broker Class."""

    ACTION = "command"

    def __init__(self, *args, saga_uuid: Optional[str] = None, reply_topic: Optional[str] = None, **kwargs):
        super().__init__(*args, **kwargs)
        self.reply_topic = reply_topic
        self.saga_uuid = saga_uuid

    @classmethod
    def _from_config(cls, *args, config: MinosConfig, **kwargs) -> CommandBroker:
        return cls(*args, **config.commands.queue._asdict(), **kwargs)

    async def send(
        self,
        items: list[MinosModel],
        topic: Optional[str] = None,
        saga_uuid: Optional[str] = None,
        reply_topic: Optional[str] = None,
        **kwargs,
    ) -> int:
        """Send a list of ``Aggregate`` instances.

        :param items: A list of aggregates.
        :param topic: Topic in which the message will be published.
        :param saga_uuid: Saga identifier.
        :param reply_topic: Topic name in which the reply will be published.
        :return: This method does not return anything.
        """
        if topic is None:
            topic = self.topic
        if saga_uuid is None:
            saga_uuid = self.saga_uuid
        if reply_topic is None:
            reply_topic = self.reply_topic
        command = Command(topic, items, saga_uuid, reply_topic)
        logger.info(f"Sending '{command!s}'...")
        return await self.send_bytes(command.topic, command.avro_bytes)
