from __future__ import (
    annotations,
)

import logging
from abc import (
    ABC,
    abstractmethod,
)
from collections.abc import (
    Iterable,
)
from typing import (
    Any,
    TypeVar,
)

from minos.common import (
    MinosConfig,
    MinosSetup,
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


class BrokerSubscriberQueueBuilder(MinosSetup, ABC):
    """Broker Subscriber Queue Builder class."""

    def __init__(self):
        super().__init__()
        self.kwargs = dict()

    def copy(self: type[B]) -> B:
        """Get a copy of the instance.

        :return: A ``BrokerSubscriberBuilder`` instance.
        """
        return self.new().with_kwargs(self.kwargs)

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

    def with_topics(self: B, topics: Iterable[str]) -> B:
        """Set topics.

        :param topics: The topics to be set.
        :return: This method return the builder instance.
        """
        self.kwargs["topics"] = set(topics)
        return self

    @abstractmethod
    def build(self: B) -> BrokerSubscriberQueue:
        """Build the instance.

        :return: A ``BrokerSubscriberQueue`` instance.
        """


B = TypeVar("B", bound=BrokerSubscriberQueueBuilder)
