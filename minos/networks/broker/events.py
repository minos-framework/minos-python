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
    Event,
    MinosConfig,
)

from .abc import (
    MinosBroker,
)


class MinosEventBroker(MinosBroker):
    """Minos Event broker class."""

    ACTION = "event"

    @classmethod
    def from_config(cls, *args, config: MinosConfig = None, **kwargs) -> Optional[MinosEventBroker]:
        """Build a new repository from config.
        :param args: Additional positional arguments.
        :param config: Config instance. If `None` is provided, default config is chosen.
        :param kwargs: Additional named arguments.
        :return: A `MinosRepository` instance.
        """
        if config is None:
            config = MinosConfig.get_default()
        if config is None:
            return None
        # noinspection PyProtectedMember
        return cls(*args, **config.events.queue._asdict(), **kwargs)

    async def send(self, items: list[Aggregate]) -> int:
        """Send a list of ``Aggregate`` instances.

        :param items: A list of aggregates.
        :return: This method does not return anything.
        """
        event = Event(self.topic, items)
        return await self._send_bytes(event.topic, event.avro_bytes)
