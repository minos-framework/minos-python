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
    TypeVar,
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
    """Broker Subscriber Builder class."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.kwargs = dict()

    @classmethod
    def new(cls: type[B]) -> B:
        """Get a new instance.

        :return: A ``BrokerSubscriberBuilder`` instance.
        """
        return cls()

    def with_kwargs(self: B, kwargs: dict[str, Any]) -> B:
        """Set kwargs.

        :param kwargs: The kwargs to be set.
        :return: This method return the builder instance.
        """
        self.kwargs |= kwargs
        return self

    def with_config(self: B, config: MinosConfig) -> B:
        """Set config.

        :param config: The config to be set.
        :return: This method return the builder instance.
        """
        return self

    def with_group_id(self: B, group_id: Optional[str]) -> B:
        """Set group_id.

        :param group_id: The group_id to be set.
        :return: This method return the builder instance.
        """
        self.kwargs["group_id"] = group_id
        return self

    def with_remove_topics_on_destroy(self: B, remove_topics_on_destroy: bool) -> B:
        """Set remove_topics_on_destroy.

        :param remove_topics_on_destroy: The remove_topics_on_destroy flag to be set.
        :return: This method return the builder instance.
        """
        self.kwargs["remove_topics_on_destroy"] = remove_topics_on_destroy
        return self

    def with_topics(self: B, topics: Iterable[str]) -> B:
        """Set topics.

        :param topics: The topics to be set.
        :return: This method return the builder instance.
        """
        self.kwargs["topics"] = set(topics)
        return self

    @abstractmethod
    def build(self) -> BrokerSubscriber:
        """Build the instance.

        :return: A ``BrokerSubscriber`` instance.
        """


B = TypeVar("B", bound=BrokerSubscriberBuilder)
