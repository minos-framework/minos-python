# Copyright (C) 2020 Clariteia SL
#
# This file is part of minos framework.
#
# Minos framework can not be copied and/or distributed without the express
# permission of Clariteia SL.

from __future__ import (
    annotations,
)

from minos.common import (
    MinosConfig,
)

from ..abc import (
    Consumer,
)


class EventConsumer(Consumer):
    """Event Consumer class."""

    TABLE_NAME = "event_queue"

    @classmethod
    def _from_config(cls, *args, config: MinosConfig, **kwargs) -> EventConsumer:
        topics = [item.name for item in config.events.items]
        return cls(topics=topics, broker=config.events.broker, **config.events.queue._asdict(), **kwargs)
