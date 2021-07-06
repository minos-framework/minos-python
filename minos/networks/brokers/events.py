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
    Aggregate,
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

    async def send(self, items: list[Aggregate], topic: Optional[str] = None, **kwargs) -> int:
        """Send a list of ``Aggregate`` instances.

        :param items: A list of aggregates.
        :param topic: Topic in which the message will be published.
        :return: This method does not return anything.
        """
        if topic is None:
            topic = self.topic
        event = Event(topic, items)
        logger.info(f"Sending '{event!s}'...")
        return await self.send_bytes(event.topic, event.avro_bytes)
