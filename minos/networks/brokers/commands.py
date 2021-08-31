"""minos.networks.brokers.commands module."""

from __future__ import (
    annotations,
)

import logging
from typing import (
    Any,
    Optional,
)
from uuid import (
    UUID,
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

    def __init__(self, *args, default_reply_topic: str, **kwargs):
        super().__init__(*args, **kwargs)
        self.default_reply_topic = default_reply_topic

    @classmethod
    def _from_config(cls, *args, config: MinosConfig, **kwargs) -> CommandBroker:
        default_reply_topic = f"{config.service.name}Reply"
        return cls(*args, **config.broker.queue._asdict(), default_reply_topic=default_reply_topic, **kwargs)

    # noinspection PyMethodOverriding
    async def send(self, data: Any, topic: str, saga: UUID, reply_topic: Optional[str] = None, **kwargs) -> int:
        """Send a ``Command``.

        :param data: The data to be send.
        :param topic: Topic in which the message will be published.
        :param saga: Saga identifier.
        :param reply_topic: Topic name in which the reply will be published.
        :return: This method does not return anything.
        """
        if reply_topic is None:
            reply_topic = self.default_reply_topic
        command = Command(topic, data, saga, reply_topic)
        logger.info(f"Sending '{command!s}'...")
        return await self.enqueue(command.topic, command.avro_bytes)
