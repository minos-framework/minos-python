from __future__ import (
    annotations,
)

import logging
from abc import (
    ABC,
    abstractmethod,
)
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

from .clients import (
    BrokerClient,
    InMemoryQueuedKafkaBroker,
    KafkaBroker,
    PostgreSqlQueuedKafkaBroker,
)
from .messages import (
    REQUEST_REPLY_TOPIC_CONTEXT_VAR,
)

logger = logging.getLogger(__name__)


class BrokerClientPool(MinosPool):
    """Dynamic BrokerClient Pool class."""

    def __init__(self, config: MinosConfig, maxsize: int = 5, recycle: Optional[int] = 3600, *args, **kwargs):
        super().__init__(maxsize=maxsize, recycle=recycle, *args, **kwargs)
        self.config = config

    @classmethod
    def _from_config(cls, config: MinosConfig, **kwargs) -> BrokerClientPool:
        return cls(config, **kwargs)

    async def _create_instance(self) -> BrokerClient:
        instance = self._broker_cls().from_config(self.config)
        await instance.setup()
        return instance

    async def _destroy_instance(self, instance: BrokerClient):
        await instance.destroy()

    def acquire(self, *args, **kwargs) -> AsyncContextManager:
        """Acquire a new instance wrapped on an asynchronous context manager.

        :return: An asynchronous context manager.
        """
        return _ReplyTopicContextManager(super().acquire())

    # noinspection PyPropertyDefinition
    @staticmethod
    @abstractmethod
    def _broker_cls() -> type[BrokerClient]:
        """TODO

        :return: TODO
        """


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


class KafkaBrokerPool(BrokerClientPool):
    """TODO"""

    # noinspection PyPropertyDefinition
    @staticmethod
    def _broker_cls() -> type[BrokerClient]:
        return KafkaBroker


class InMemoryQueuedKafkaBrokerPool(BrokerClientPool):
    """TODO"""

    # noinspection PyPropertyDefinition
    @staticmethod
    def _broker_cls() -> type[BrokerClient]:
        return InMemoryQueuedKafkaBroker


class PostgreSqlQueuedKafkaBrokerPool(BrokerClientPool):
    """TODO"""

    # noinspection PyPropertyDefinition
    @staticmethod
    def _broker_cls() -> type[BrokerClient]:
        return PostgreSqlQueuedKafkaBroker
