"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from __future__ import (
    annotations,
)

import logging
from typing import (
    Optional,
)
from uuid import (
    uuid4,
)

from dependency_injector.wiring import (
    Provide,
)
from kafka import (
    KafkaAdminClient,
)
from kafka.admin import (
    NewTopic,
)

from minos.common import (
    MinosConfig,
    MinosPool,
)

from ..consumers import (
    Consumer,
)
from .handlers import (
    DynamicReplyHandler,
)

logger = logging.getLogger(__name__)


class ReplyHandlerPool(MinosPool):
    """Reply Handler Pool class."""

    consumer: Consumer = Provide["consumer"]

    def __init__(
        self, config: MinosConfig, client: KafkaAdminClient, consumer: Optional[Consumer] = None, *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.config = config
        self.client = client

        if consumer is not None:
            self.consumer = consumer

    @classmethod
    def _from_config(cls, *args, config: MinosConfig, **kwargs) -> ReplyHandlerPool:
        client = KafkaAdminClient(bootstrap_servers=f"{config.broker.host}:{config.broker.port}")
        return cls(config, client, **kwargs)

    async def _create_instance(self) -> DynamicReplyHandler:
        topic = str(uuid4()).replace("-", "")

        await self._create_reply_topic(topic)

        instance = DynamicReplyHandler.from_config(config=self.config, topic=topic)
        await instance.setup()

        self.consumer.add_topic(f"{topic}Reply")

        return instance

    async def _destroy_instance(self, instance: DynamicReplyHandler):
        self.consumer.remove_topic(f"{instance.topic}Reply")

        await instance.destroy()

        await self._delete_reply_topic(instance.topic)

    async def _create_reply_topic(self, topic: str):
        name = f"{topic}Reply"
        logger.info(f"Creating {name!r} topic...")
        self.client.create_topics([NewTopic(name=name, num_partitions=1, replication_factor=1)])

    async def _delete_reply_topic(self, topic: str):
        name = f"{topic}Reply"
        logger.info(f"Deleting {name!r} topic...")
        self.client.delete_topics([name])
