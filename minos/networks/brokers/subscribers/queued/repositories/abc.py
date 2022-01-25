import logging
from abc import (
    ABC,
    abstractmethod,
)
from collections.abc import (
    AsyncIterator,
)
from typing import (
    Iterable,
)

from minos.common import (
    MinosSetup,
)

from ....messages import (
    BrokerMessage,
)

logger = logging.getLogger(__name__)


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

    async def enqueue(self, message: BrokerMessage) -> None:
        """Enqueue a new message.

        :param message: The ``BrokerMessage`` to be enqueued.
        :return: This method does not return anything.
        """
        logger.info(f"Enqueueing {message!r} message...")
        await self._enqueue(message)

    @abstractmethod
    async def _enqueue(self, message: BrokerMessage) -> None:
        raise NotImplementedError

    async def dequeue(self) -> BrokerMessage:
        """Dequeue a message from the queue.

        :return: The dequeued ``BrokerMessage``.
        """
        message = await self._dequeue()
        logger.info(f"Dequeuing {message!r} message...")
        return message

    @abstractmethod
    async def _dequeue(self) -> BrokerMessage:
        raise NotImplementedError
