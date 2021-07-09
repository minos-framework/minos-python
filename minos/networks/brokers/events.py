"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from __future__ import (
    annotations,
)

import logging

from minos.common import (
    AggregateDiff,
    Event,
    MinosConfig,
)

from .abc import (
    Broker,
)

logger = logging.getLogger(__name__)


class EventBroker(Broker):
    """Minos Event broker class."""

    ACTION = "event"

    @classmethod
    def _from_config(cls, *args, config: MinosConfig, **kwargs) -> EventBroker:
        return cls(*args, **config.events.queue._asdict(), **kwargs)

    # noinspection PyMethodOverriding
    async def send(self, data: AggregateDiff, topic: str, **kwargs) -> int:
        """Send an ``Event``.

        :param data: The data to be send.
        :param topic: Topic in which the message will be published.
        :return: This method does not return anything.
        """
        event = Event(topic, data)
        logger.info(f"Sending '{event!s}'...")
        return await self.send_bytes(event.topic, event.avro_bytes)
