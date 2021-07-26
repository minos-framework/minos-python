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
from ..decorators import (
    EnrouteBuilder,
)


class CommandConsumer(Consumer):
    """Command Consumer class."""

    TABLE_NAME = "command_queue"

    @classmethod
    def _from_config(cls, *args, config: MinosConfig, **kwargs) -> CommandConsumer:
        command_decorators = EnrouteBuilder(config.commands.service).get_broker_command_query()
        query_decorators = EnrouteBuilder(config.queries.service).get_broker_command_query()

        topics = {decorator.topic for decorator in chain(command_decorators.keys(), query_decorators.keys())}

        return cls(topics=topics, broker=config.commands.broker, **config.commands.queue._asdict(), **kwargs)
