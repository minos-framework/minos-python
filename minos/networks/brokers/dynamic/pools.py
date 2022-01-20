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

    def __init__(self, config: MinosConfig, maxsize: int = 5, recycle: Optional[int] = 3600, *args, **kwargs):
        super().__init__(maxsize=maxsize, recycle=recycle, *args, **kwargs)
        self.config = config

    @classmethod
    def _from_config(cls, config: MinosConfig, **kwargs) -> DynamicBrokerPool:
        return cls(config, **kwargs)

    async def _create_instance(self) -> DynamicBroker:
        instance = DynamicBroker.from_config(self.config)
        await instance.setup()
        return instance

    async def _destroy_instance(self, instance: DynamicBroker):
        await instance.destroy()

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
        broker = await self.wrapper.__aenter__()
        self._token = REQUEST_REPLY_TOPIC_CONTEXT_VAR.set(broker.topic)
        return broker

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        REQUEST_REPLY_TOPIC_CONTEXT_VAR.reset(self._token)
        await self.wrapper.__aexit__(exc_type, exc_val, exc_tb)
