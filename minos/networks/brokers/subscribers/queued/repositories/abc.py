import logging
from abc import (
    ABC,
)
from collections.abc import (
    Iterable,
)

from ....collections import (
    BrokerRepository,
)

logger = logging.getLogger(__name__)


class BrokerSubscriberRepository(BrokerRepository, ABC):
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
