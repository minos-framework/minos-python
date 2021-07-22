# Copyright (C) 2020 Clariteia SL
#
# This file is part of minos framework.
#
# Minos framework can not be copied and/or distributed without the express
# permission of Clariteia SL.
from __future__ import annotations

from minos.common import MinosConfig

from ..abc import Consumer

from ..decorators import EnrouteDecoratorAnalyzer

from importlib import import_module


class CommandConsumer(Consumer):
    """Command Consumer class."""

    TABLE_NAME = "command_queue"

    @classmethod
    def _from_config(cls, *args, config: MinosConfig, **kwargs) -> CommandConsumer:
        p, m = config.commands.service.rsplit(".", 1)
        mod = import_module(p)
        met = getattr(mod, m)

        decorators = EnrouteDecoratorAnalyzer(met).command()

        topics = []
        for key, value in decorators.items():
            for v in decorators[key]:
                topics.extend(v.topics)

        return cls(topics=topics, broker=config.commands.broker, **config.commands.queue._asdict(), **kwargs)
