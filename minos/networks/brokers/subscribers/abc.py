from __future__ import (
    annotations,
)

from abc import (
    ABC,
    abstractmethod,
)
from collections.abc import (
    AsyncIterator,
    Iterable,
)
from typing import (
    Any,
    Optional,
)

from minos.common import (
    MinosConfig,
    MinosSetup,
)

from ..messages import (
    BrokerMessage,
)


class BrokerSubscriber(ABC, MinosSetup):
    """Broker Subscriber class."""

    def __init__(self, topics: Iterable[str], **kwargs):
        super().__init__(**kwargs)
        self._topics = set(topics)

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
        return await self.receive()

    @abstractmethod
    async def receive(self) -> BrokerMessage:
        """Receive a new message.

        :return: A ``BrokerMessage`` instance.
        """


class BrokerSubscriberBuilder(MinosSetup, ABC):
    """TODO"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.kwargs = dict()

    @classmethod
    def new(cls) -> BrokerSubscriberBuilder:
        """TODO"""
        return cls()

    def with_kwargs(self, kwargs: dict[str, Any]) -> BrokerSubscriberBuilder:
        """TODO"""
        self.kwargs |= kwargs
        return self

    def with_config(self, config: MinosConfig) -> BrokerSubscriberBuilder:
        """TODO"""
        return self

    def with_group_id(self, group_id: Optional[str]) -> BrokerSubscriberBuilder:
        """TODO"""
        self.kwargs["group_id"] = group_id
        return self

    def with_remove_topics_on_destroy(self, remove_topics_on_destroy: bool) -> BrokerSubscriberBuilder:
        """TODO"""
        self.kwargs["remove_topics_on_destroy"] = remove_topics_on_destroy
        return self

    def with_topics(self, topics: Iterable[str]) -> BrokerSubscriberBuilder:
        """TODO"""
        self.kwargs["topics"] = set(topics)
        return self

    @abstractmethod
    def build(self) -> BrokerSubscriber:
        """TODO"""
