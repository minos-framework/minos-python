from __future__ import (
    annotations,
)

from abc import (
    ABC,
    abstractmethod,
)
from collections.abc import (
    AsyncIterator,
)
from typing import (
    Any,
    Iterable,
    TypeVar,
)

from minos.common import (
    MinosConfig,
    MinosSetup,
)

from ....messages import (
    BrokerMessage,
)


class BrokerSubscriberRepository(ABC, MinosSetup):
    """Broker Subscriber Repository class."""

    def __init__(self, topics: Iterable[str], **kwargs):
        super().__init__(**kwargs)
        topics = set(topics)
        if not len(topics):
            raise ValueError("The topics set must not be empty.")
        self._topics = topics

    @property
    def topics(self) -> set[str]:
        """Topics getter.

        :return: A list of string values.
        """
        return self._topics

    def __aiter__(self) -> AsyncIterator[BrokerMessage]:
        return self

    async def __anext__(self) -> BrokerMessage:
        if self.already_destroyed:
            raise StopAsyncIteration
        return await self.dequeue()

    @abstractmethod
    async def enqueue(self, message: BrokerMessage) -> None:
        """Enqueue a new message.

        :param message: The ``BrokerMessage`` to be enqueued.
        :return: This method does not return anything.
        """

    @abstractmethod
    async def dequeue(self) -> BrokerMessage:
        """Dequeue a message from the queue.

        :return: The dequeued ``BrokerMessage``.
        """


class BrokerSubscriberRepositoryBuilder(MinosSetup, ABC):
    """TODO"""

    def __init__(self):
        super().__init__()
        self.kwargs = dict()

    @classmethod
    def new(cls: type[B]) -> B:
        """TODO"""
        return cls()

    def with_kwargs(self: B, kwargs: dict[str, Any]) -> B:
        """TODO"""
        self.kwargs |= kwargs
        return self

    def with_config(self: B, config: MinosConfig) -> B:
        """TODO"""
        return self

    def with_topics(self: B, topics: Iterable[str]) -> B:
        """TODO"""
        self.kwargs["topics"] = set(topics)
        return self

    @abstractmethod
    def build(self: B) -> BrokerSubscriberRepository:
        """TODO"""


B = TypeVar("B", bound=BrokerSubscriberRepositoryBuilder)
