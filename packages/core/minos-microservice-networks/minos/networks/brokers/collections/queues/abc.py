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
)

from minos.common import (
    BuildableMixin,
)

from ...messages import (
    BrokerMessage,
)

logger = logging.getLogger(__name__)


class BrokerQueue(ABC, BuildableMixin):
    """Broker Queue class."""

    async def enqueue(self, message: BrokerMessage) -> None:
        """Enqueue method."""
        logger.debug(f"Enqueuing {message!r} message...")
        await self._enqueue(message)

    @abstractmethod
    async def _enqueue(self, message: BrokerMessage) -> None:
        raise NotImplementedError

    def __aiter__(self) -> AsyncIterator[BrokerMessage]:
        return self

    async def __anext__(self) -> BrokerMessage:
        if self.already_destroyed:
            raise StopAsyncIteration
        return await self.dequeue()

    async def dequeue(self) -> BrokerMessage:
        """Dequeue method."""
        message = await self._dequeue()
        logger.debug(f"Dequeuing {message!r} message...")
        return message

    @abstractmethod
    async def _dequeue(self) -> BrokerMessage:
        raise NotImplementedError
