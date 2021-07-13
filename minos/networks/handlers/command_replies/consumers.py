# Copyright (C) 2020 Clariteia SL
#
# This file is part of minos framework.
#
# Minos framework can not be copied and/or distributed without the express
# permission of Clariteia SL.
from __future__ import (
    annotations,
)

from itertools import (
    chain,
)

from minos.common import (
    MinosConfig,
)

from ..abc import (
    Consumer,
)


class CommandReplyConsumer(Consumer):
    """Command Reply consumer class."""

    TABLE_NAME = "command_reply_queue"

    @classmethod
    def _from_config(cls, *args, config: MinosConfig, **kwargs) -> CommandReplyConsumer:
        iterable = chain(config.saga.items, config.events.items)
        topics = [f"{item.name}Reply" for item in iterable]
        return cls(topics=topics, broker=config.saga.broker, **config.saga.queue._asdict(), **kwargs)
