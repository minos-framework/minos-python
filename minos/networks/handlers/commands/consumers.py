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
    Command,
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
        kafka_conn_data = f"{config.commands.broker.host}:{config.commands.broker.port}"
        return cls(topics=topics, kafka_conn_data=kafka_conn_data, **config.commands.queue._asdict(), **kwargs)

    def _is_valid_instance(self, value: bytes):
        try:
            Command.from_avro_bytes(value)
            return True
        except:  # noqa E722
            return False
