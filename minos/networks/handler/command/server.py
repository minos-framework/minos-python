# Copyright (C) 2020 Clariteia SL
#
# This file is part of minos framework.
#
# Minos framework can not be copied and/or distributed without the express
# permission of Clariteia SL.

from typing import (
    Any,
)

from minos.common import (
    Command,
    MinosConfig,
)

from ..server import (
    MinosHandlerServer,
)


class MinosCommandHandlerServer(MinosHandlerServer):

    TABLE = "command_queue"

    def __init__(self, *, config: MinosConfig, **kwargs: Any):
        super().__init__(table_name=self.TABLE, config=config.commands, **kwargs)
        self._kafka_conn_data = f"{config.commands.broker.host}:{config.commands.broker.port}"
        self._broker_group_name = f"event_{config.service.name}"

    def _is_valid_instance(self, value: bytes):
        try:
            Command.from_avro_bytes(value)
            return True
        except:  # noqa E722
            return False
