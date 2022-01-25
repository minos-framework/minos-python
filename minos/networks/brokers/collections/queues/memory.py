import logging
from asyncio import (
    Queue,
    QueueEmpty,
    TimeoutError,
    wait_for,
)

from ...messages import (
    BrokerMessage,
)
from .abc import (
    BrokerQueue,
)

logger = logging.getLogger(__name__)


class InMemoryBrokerQueue(BrokerQueue):
    """In Memory Broker Queue class."""

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

    async def _enqueue(self, message: BrokerMessage) -> None:
        await self._queue.put(message)

    async def _dequeue(self) -> BrokerMessage:
        message = await self._queue.get()
        self._queue.task_done()
        return message
