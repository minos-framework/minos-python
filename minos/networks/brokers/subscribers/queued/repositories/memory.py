from asyncio import (
    Queue,
)
from collections.abc import (
    AsyncIterator,
)

from ....messages import (
    BrokerMessage,
)
from .abc import (
    BrokerSubscriberRepository,
)


class InMemoryBrokerSubscriberRepository(BrokerSubscriberRepository):
    """TODO"""

    queue: Queue[BrokerMessage]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.queue = Queue()

    async def enqueue(self, message: BrokerMessage) -> None:
        """Enqueue method."""
        await self.queue.put(message)

    async def dequeue_all(self) -> AsyncIterator[BrokerMessage]:
        """Dequeue all method."""
        while True:
            message = await self.queue.get()
            yield message
