import logging
from asyncio import (
    Queue,
)
from typing import (
    Awaitable,
)

from ....messages import (
    BrokerMessage,
)
from .abc import (
    BrokerPublisherRepository,
)

logger = logging.getLogger(__name__)


class InMemoryBrokerPublisherRepository(BrokerPublisherRepository):
    """In Memory Broker Publisher Repository class."""

    queue: Queue[BrokerMessage]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.queue = Queue()

    async def enqueue(self, message: BrokerMessage) -> None:
        """Enqueue method."""
        logger.info(f"Enqueuing {message!r} message...")
        await self.queue.put(message)

    def dequeue(self) -> Awaitable[BrokerMessage]:
        """Dequeue method."""
        return self.queue.get()
