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
    CommandReply,
    MinosConfig,
)

from ..dispatcher import (
    MinosHandlerDispatcher,
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
        except:  # noqa E722
            return False, None
