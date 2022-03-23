from __future__ import (
    annotations,
)

import logging
from abc import (
    ABC,
)
from collections.abc import (
    Iterable,
)
from typing import (
    Generic,
    TypeVar,
)

from minos.common import (
    Builder,
)

from ....collections import (
    BrokerQueue,
)

logger = logging.getLogger(__name__)


class BrokerSubscriberQueue(BrokerQueue, ABC):
    """Broker Subscriber Queue class."""

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


BrokerSubscriberQueueCls = TypeVar("BrokerSubscriberQueueCls", bound=BrokerSubscriberQueue)


class BrokerSubscriberQueueBuilder(Builder[BrokerSubscriberQueueCls], Generic[BrokerSubscriberQueueCls]):
    """Broker Subscriber Queue Builder class."""

    def with_topics(self: B, topics: Iterable[str]) -> B:
        """Set topics.

        :param topics: The topics to be set.
        :return: This method return the builder instance.
        """
        self.kwargs["topics"] = set(topics)
        return self


BrokerSubscriberQueue.set_builder(BrokerSubscriberQueueBuilder)

B = TypeVar("B", bound=BrokerSubscriberQueueBuilder)
