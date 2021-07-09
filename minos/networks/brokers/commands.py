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
    Any,
)

from minos.common import (
    Command,
    MinosConfig,
)

from .abc import (
    Broker,
)

logger = logging.getLogger(__name__)


class CommandBroker(Broker):
    """Minos Command Broker Class."""

    ACTION = "command"

    @classmethod
    def _from_config(cls, *args, config: MinosConfig, **kwargs) -> CommandBroker:
        return cls(*args, **config.commands.queue._asdict(), **kwargs)

    # noinspection PyMethodOverriding
    async def send(self, data: Any, topic: str, saga: str, reply_topic: str, **kwargs) -> int:
        """Send a ``Command``.

        :param data: The data to be send.
        :param topic: Topic in which the message will be published.
        :param saga: Saga identifier.
        :param reply_topic: Topic name in which the reply will be published.
        :return: This method does not return anything.
        """
        command = Command(topic, data, saga, reply_topic)
        logger.info(f"Sending '{command!s}'...")
        return await self.send_bytes(command.topic, command.avro_bytes)
