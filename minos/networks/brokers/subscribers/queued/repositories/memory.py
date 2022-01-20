import logging
from asyncio import Queue

from ....messages import BrokerMessage
from .abc import BrokerSubscriberRepository

logger = logging.getLogger(__name__)


class InMemoryBrokerSubscriberRepository(BrokerSubscriberRepository):
    """TODO"""

    queue: Queue[BrokerMessage]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.queue = Queue()

    async def enqueue(self, message: BrokerMessage) -> None:
        """Enqueue method."""

        logger.info(f"Enqueueing {message!r} message...")
        await self.queue.put(message)

    async def dequeue(self) -> BrokerMessage:
        """Dequeue all method."""
        message = await self.queue.get()
        logger.info(f"Dequeuing {message!r} message...")
        return message
