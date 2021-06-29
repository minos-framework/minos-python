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


class CommandConsumer(Consumer):
    """Command Consumer class."""

    TABLE_NAME = "command_queue"

    @classmethod
    def _from_config(cls, *args, config: MinosConfig, **kwargs) -> CommandConsumer:
        topics = [item.name for item in config.commands.items]
        return cls(topics=topics, broker=config.commands.broker, **config.commands.queue._asdict(), **kwargs)
