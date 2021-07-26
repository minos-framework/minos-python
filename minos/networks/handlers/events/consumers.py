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

from ...decorators import (
    EnrouteBuilder,
)
from ..abc import (
    Consumer,
)


class EventConsumer(Consumer):
    """Event Consumer class."""

    TABLE_NAME = "event_queue"

    @classmethod
    def _from_config(cls, *args, config: MinosConfig, **kwargs) -> EventConsumer:
        decorators = EnrouteBuilder(config.events.service).get_broker_event()

        topics = {decorator.topic for decorator in decorators.keys()}

        return cls(topics=topics, broker=config.events.broker, **config.events.queue._asdict(), **kwargs)
