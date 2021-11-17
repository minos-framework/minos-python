from __future__ import (
    annotations,
)

import logging
from typing import (
    Any,
)

from minos.common import (
    MinosConfig,
)

from ..messages import (
    Event,
)
from .abc import (
    BrokerPublisher,
)

logger = logging.getLogger(__name__)


class EventBrokerPublisher(BrokerPublisher):
    """Minos Event broker class."""

    ACTION = "event"

    @classmethod
    def _from_config(cls, *args, config: MinosConfig, **kwargs) -> EventBrokerPublisher:
        return cls(*args, **config.broker.queue._asdict(), **kwargs)

    # noinspection PyMethodOverriding
    async def send(self, data: Any, topic: str, **kwargs) -> int:
        """Send an ``Event``.

        :param data: The data to be send.
        :param topic: Topic in which the message will be published.
        :return: This method does not return anything.
        """
        event = Event(topic, data)
        logger.info(f"Sending '{event!s}'...")
        return await self.enqueue(event.topic, event.avro_bytes)
