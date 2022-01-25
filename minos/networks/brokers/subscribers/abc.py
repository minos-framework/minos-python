import logging
from abc import (
    ABC,
    abstractmethod,
)
from collections.abc import (
    AsyncIterator,
    Iterable,
)

from minos.common import (
    MinosSetup,
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
        logger.info(f"Receiving {message!r} message...")
        return message

    @abstractmethod
    async def _receive(self) -> BrokerMessage:
        raise NotImplementedError
