# Copyright (C) 2020 Clariteia SL
#
# This file is part of minos framework.
#
# Minos framework can not be copied and/or distributed without the express
# permission of Clariteia SL.

from __future__ import annotations
from importlib import import_module
from minos.common import MinosConfig

from ..abc import Consumer
from ..decorators import EnrouteDecoratorAnalyzer


class EventConsumer(Consumer):
    """Event Consumer class."""

    TABLE_NAME = "event_queue"

    @classmethod
    def _from_config(cls, *args, config: MinosConfig, **kwargs) -> EventConsumer:
        p, m = config.events.service.rsplit(".", 1)
        mod = import_module(p)
        met = getattr(mod, m)

        decorators = EnrouteDecoratorAnalyzer(met).event()

        topics = []
        for key, value in decorators.items():
            for v in decorators[key]:
                topics.extend(v.topics)

        return cls(topics=topics, broker=config.events.broker, **config.events.queue._asdict(), **kwargs)
