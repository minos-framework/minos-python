from __future__ import (
    annotations,
)

import logging
from contextvars import (
    Token,
)
from typing import (
    Any,
    AsyncContextManager,
    Optional,
)

from minos.common import (
    Config,
    Pool,
)

from .clients import (
    BrokerClient,
)
from .messages import (
    REQUEST_REPLY_TOPIC_CONTEXT_VAR,
)

logger = logging.getLogger(__name__)


class BrokerClientPool(Pool):
    """Broker Client Pool class."""

    def __init__(self, instance_kwargs: dict[str, Any], maxsize: int = 5, *args, **kwargs):
        super().__init__(maxsize=maxsize, *args, **kwargs)
        self._instance_kwargs = instance_kwargs

    @classmethod
    def _from_config(cls, config: Config, **kwargs) -> BrokerClientPool:
        return cls(kwargs | {"config": config})

    async def _create_instance(self) -> BrokerClient:
        instance = BrokerClient.from_config(**self._instance_kwargs)
        await instance.setup()
        return instance

    async def _destroy_instance(self, instance: BrokerClient):
        await instance.destroy()

    def acquire(self, *args, **kwargs) -> AsyncContextManager:
        """Acquire a new instance wrapped on an asynchronous context manager.

        :return: An asynchronous context manager.
        """
        return _ReplyTopicContextManager(super().acquire())


class _ReplyTopicContextManager:
    _token: Optional[Token]

    def __init__(self, wrapper: AsyncContextManager[BrokerClient]):
        self.wrapper = wrapper
        self._token = None

    async def __aenter__(self) -> BrokerClient:
        broker = await self.wrapper.__aenter__()
        self._token = REQUEST_REPLY_TOPIC_CONTEXT_VAR.set(broker.topic)
        return broker

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        REQUEST_REPLY_TOPIC_CONTEXT_VAR.reset(self._token)
        await self.wrapper.__aexit__(exc_type, exc_val, exc_tb)
