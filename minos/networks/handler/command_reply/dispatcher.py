# Copyright (C) 2020 Clariteia SL
#
# This file is part of minos framework.
#
# Minos framework can not be copied and/or distributed without the express
# permission of Clariteia SL.
import collections

from minos.common.configuration.config import (
    MinosConfig,
)
from typing import (
    Any,
)
from ..dispatcher import (
    MinosHandlerDispatcher,
)
from minos.common import (
    CommandReply,
)


class MinosCommandReplyHandlerDispatcher(MinosHandlerDispatcher):

    TABLE = "command_reply_queue"

    def __init__(self, *, config: MinosConfig, **kwargs: Any):
        super().__init__(table_name=self.TABLE, config=config.saga, **kwargs)
        self._broker_group_name = f"event_{config.service.name}"

    def _is_valid_instance(self, value: bytes):
        try:
            instance = CommandReply.from_avro_bytes(value)
            return True, instance
        except:
            return False, None
