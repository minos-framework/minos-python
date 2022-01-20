from __future__ import (
    annotations,
)

import logging
from contextvars import (
    Token,
)
from typing import (
    AsyncContextManager,
    Optional,
)
from uuid import (
    uuid4,
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

from ..messages import (
    REQUEST_REPLY_TOPIC_CONTEXT_VAR,
)
from .brokers import (
    DynamicBroker,
)

logger = logging.getLogger(__name__)


class DynamicBrokerPool(MinosPool):
    """Dynamic Broker Pool class."""

    def __init__(
        self,
        config: MinosConfig,
        client: KafkaAdminClient,
        maxsize: int = 5,
        recycle: Optional[int] = 3600,
        *args,
        **kwargs,
    ):
        super().__init__(maxsize=maxsize, recycle=recycle, *args, **kwargs)
        self.config = config
        self.client = client

    @classmethod
    def _from_config(cls, config: MinosConfig, **kwargs) -> DynamicBrokerPool:
        kwargs["client"] = KafkaAdminClient(bootstrap_servers=f"{config.broker.host}:{config.broker.port}")
        return cls(config, **kwargs)

    async def _destroy(self) -> None:
        await super()._destroy()
        self.client.close()

    async def _create_instance(self) -> DynamicBroker:
        topic = str(uuid4()).replace("-", "")
        await self._create_reply_topic(topic)
        instance = DynamicBroker.from_config(self.config, topic=topic)
        await instance.setup()
        return instance

    async def _destroy_instance(self, instance: DynamicBroker):
        await instance.destroy()
        await self._delete_reply_topic(instance.topic)

    async def _create_reply_topic(self, topic: str) -> None:
        logger.info(f"Creating {topic!r} topic...")
        self.client.create_topics([NewTopic(name=topic, num_partitions=1, replication_factor=1)])

    async def _delete_reply_topic(self, topic: str) -> None:
        logger.info(f"Deleting {topic!r} topic...")
        self.client.delete_topics([topic])

    def acquire(self, *args, **kwargs) -> AsyncContextManager:
        """Acquire a new instance wrapped on an asynchronous context manager.

        :return: An asynchronous context manager.
        """
        return _ReplyTopicContextManager(super().acquire())


class _ReplyTopicContextManager:
    _token: Optional[Token]

    def __init__(self, wrapper: AsyncContextManager[DynamicBroker]):
        self.wrapper = wrapper
        self._token = None

    async def __aenter__(self) -> DynamicBroker:
        handler = await self.wrapper.__aenter__()
        self._token = REQUEST_REPLY_TOPIC_CONTEXT_VAR.set(handler.topic)
        return handler

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        REQUEST_REPLY_TOPIC_CONTEXT_VAR.reset(self._token)
        await self.wrapper.__aexit__(exc_type, exc_val, exc_tb)
