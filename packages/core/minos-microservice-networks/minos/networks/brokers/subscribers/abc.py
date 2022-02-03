from __future__ import (
    annotations,
)

import logging
from abc import (
    ABC,
    abstractmethod,
)
from collections.abc import (
    AsyncIterator,
    Iterable,
)
from typing import (
    Optional,
)

from minos.common import (
    MinosSetup,
)

from ...utils import (
    Builder,
)
from ..messages import (
    BrokerMessage,
)

logger = logging.getLogger(__name__)


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

    async def receive(self) -> BrokerMessage:
        """Receive a new message.

        :return: A ``BrokerMessage`` instance.
        """
        message = await self._receive()
        logger.debug(f"Receiving {message!r} message...")
        return message

    @abstractmethod
    async def _receive(self) -> BrokerMessage:
        raise NotImplementedError


class BrokerSubscriberBuilder(Builder[BrokerSubscriber], ABC):
    """Broker Subscriber Builder class."""

    def with_group_id(self, group_id: Optional[str]):
        """Set group_id.

        :param group_id: The group_id to be set.
        :return: This method return the builder instance.
        """
        self.kwargs["group_id"] = group_id
        return self

    def with_remove_topics_on_destroy(self, remove_topics_on_destroy: bool):
        """Set remove_topics_on_destroy.

        :param remove_topics_on_destroy: The remove_topics_on_destroy flag to be set.
        :return: This method return the builder instance.
        """
        self.kwargs["remove_topics_on_destroy"] = remove_topics_on_destroy
        return self

    def with_topics(self, topics: Iterable[str]):
        """Set topics.

        :param topics: The topics to be set.
        :return: This method return the builder instance.
        """
        self.kwargs["topics"] = set(topics)
        return self
