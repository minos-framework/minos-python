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

from dependency_injector.wiring import (
    Provide,
    inject,
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

from ...brokers import (
    REPLY_TOPIC_CONTEXT_VAR,
)
from ..consumers import (
    Consumer,
)
from .handlers import (
    DynamicHandler,
)

logger = logging.getLogger(__name__)


class DynamicHandlerPool(MinosPool):
    """Dynamic Handler Pool class."""

    @inject
    def __init__(
        self,
        config: MinosConfig,
        client: KafkaAdminClient,
        maxsize: int = 5,
        consumer: Consumer = Provide["consumer"],
        *args,
        **kwargs,
    ):
        super().__init__(maxsize=maxsize, *args, **kwargs)
        self.config = config
        self.client = client
        self.consumer = consumer

    @classmethod
    def _from_config(cls, *args, config: MinosConfig, **kwargs) -> DynamicHandlerPool:
        client = KafkaAdminClient(bootstrap_servers=f"{config.broker.host}:{config.broker.port}")
        return cls(config, client, **kwargs)

    async def _create_instance(self) -> DynamicHandler:
        topic = str(uuid4()).replace("-", "")
        await self._create_reply_topic(topic)
        await self._subscribe_reply_topic(topic)
        instance = DynamicHandler.from_config(config=self.config, topic=topic)
        await instance.setup()
        return instance

    async def _destroy_instance(self, instance: DynamicHandler):
        await instance.destroy()
        await self._unsubscribe_reply_topic(instance.topic)
        await self._delete_reply_topic(instance.topic)

    async def _create_reply_topic(self, topic: str) -> None:
        logger.info(f"Creating {topic!r} topic...")
        self.client.create_topics([NewTopic(name=topic, num_partitions=1, replication_factor=1)])

    async def _delete_reply_topic(self, topic: str) -> None:
        logger.info(f"Deleting {topic!r} topic...")
        self.client.delete_topics([topic])

    async def _subscribe_reply_topic(self, topic: str) -> None:
        await self.consumer.add_topic(topic)

    async def _unsubscribe_reply_topic(self, topic: str) -> None:
        await self.consumer.remove_topic(topic)

    def acquire(self, *args, **kwargs) -> AsyncContextManager:
        """Acquire a new instance wrapped on an asynchronous context manager.

        :return: An asynchronous context manager.
        """
        return _ReplyTopicContextManager(super().acquire())


class _ReplyTopicContextManager:
    _token: Optional[Token]

    def __init__(self, wrapper: AsyncContextManager[DynamicHandler]):
        self.wrapper = wrapper
        self._token = None

    async def __aenter__(self) -> DynamicHandler:
        handler = await self.wrapper.__aenter__()
        self._token = REPLY_TOPIC_CONTEXT_VAR.set(handler.topic)
        return handler

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        REPLY_TOPIC_CONTEXT_VAR.reset(self._token)
        await self.wrapper.__aexit__(exc_type, exc_val, exc_tb)
