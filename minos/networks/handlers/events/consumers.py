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
    Event,
    MinosConfig,
)

from ..abc import (
    Consumer,
)


class EventConsumer(Consumer):
    """Event Consumer class."""

    TABLE_NAME = "event_queue"

    @classmethod
    def _from_config(cls, *args, config: MinosConfig, **kwargs) -> EventConsumer:
        topics = [item.name for item in config.events.items]
        kafka_conn_data = f"{config.events.broker.host}:{config.events.broker.port}"
        return cls(topics=topics, kafka_conn_data=kafka_conn_data, **config.events.queue._asdict(), **kwargs)

    def _is_valid_instance(self, value: bytes):
        try:
            Event.from_avro_bytes(value)
            return True
        except:  # noqa E722
            return False
