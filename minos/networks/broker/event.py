# Copyright (C) 2020 Clariteia SL
#
# This file is part of minos framework.
#
# Minos framework can not be copied and/or distributed without the express
# permission of Clariteia SL.
from __future__ import annotations

import datetime
from typing import (
    NoReturn,
    Optional,
)

from minos.common import (
    Aggregate,
    Event,
    MinosConfig,
)

from .abc import MinosBroker


class MinosEventBroker(MinosBroker):
    """TODO"""

    ACTION = "event"

    @classmethod
    def from_config(cls, *args, config: MinosConfig = None, **kwargs) -> Optional[MinosEventBroker]:
        """Build a new repository from config.
        :param args: Additional positional arguments.
        :param config: Config instance. If `None` is provided, default config is chosen.
        :param kwargs: Additional named arguments.
        :return: A `MinosRepository` instance.
        """
        if config is None:
            config = MinosConfig.get_default()
        if config is None:
            return None
        # noinspection PyProtectedMember
        return cls(*args, **config.events.queue._asdict(), **kwargs)

    async def send(self, items: list[Aggregate]) -> NoReturn:
        event_instance = Event(self.topic, items)
        bin_data = event_instance.avro_bytes

        async with self._connection() as connect:
            async with connect.cursor() as cur:
                await cur.execute(
                    "INSERT INTO producer_queue (topic, model, retry, action, creation_date, update_date) VALUES (%s, %s, %s, %s, %s, %s) RETURNING id;",
                    (event_instance.topic, bin_data, 0, self.ACTION, datetime.datetime.now(), datetime.datetime.now(),),
                )

                queue_id = await cur.fetchone()
                affected_rows = cur.rowcount

        return affected_rows, queue_id[0]
