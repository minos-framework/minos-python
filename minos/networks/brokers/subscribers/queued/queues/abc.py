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
    TypeVar,
)

from .....utils import (
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


class BrokerSubscriberQueueBuilder(Builder[BrokerSubscriberQueue], ABC):
    """Broker Subscriber Queue Builder class."""

    def with_topics(self: B, topics: Iterable[str]) -> B:
        """Set topics.

        :param topics: The topics to be set.
        :return: This method return the builder instance.
        """
        self.kwargs["topics"] = set(topics)
        return self


B = TypeVar("B", bound=BrokerSubscriberQueueBuilder)
