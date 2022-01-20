import logging
from asyncio import (
    Queue,
    QueueEmpty,
    TimeoutError,
    wait_for,
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

    _queue: Queue[BrokerMessage]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._queue = Queue()

    async def _destroy(self) -> None:
        await self._flush_queue()
        await super()._destroy()

    async def _flush_queue(self):
        try:
            await wait_for(self._queue.join(), 0.5)
        except TimeoutError:
            messages = list()
            while True:
                try:
                    messages.append(self._queue.get_nowait())
                except QueueEmpty:
                    break
            logger.warning(f"Some messages were loosed: {messages}")

    async def enqueue(self, message: BrokerMessage) -> None:
        """Enqueue method."""
        logger.info(f"Enqueuing {message!r} message...")
        await self._queue.put(message)

    async def dequeue(self) -> BrokerMessage:
        """Dequeue method."""
        message = await self._queue.get()
        logger.info(f"Dequeuing {message!r} message...")
        self._queue.task_done()
        return message
