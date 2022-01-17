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
    BrokerPublisherRepository,
)


class InMemoryBrokerPublisherRepository(BrokerPublisherRepository):
    """In Memory Broker Publisher Repository class."""

    impl: Queue[BrokerMessage]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.impl = Queue()

    async def enqueue(self, message: BrokerMessage) -> None:
        """Enqueue method."""
        await self.impl.put(message)

    async def dequeue_all(self) -> AsyncIterator[BrokerMessage]:
        """Dequeue all method."""
        while True:
            message = await self.impl.get()
            yield message
