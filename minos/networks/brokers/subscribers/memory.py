from asyncio import (
    Queue,
)
from collections.abc import (
    Iterable,
)

from ..messages import (
    BrokerMessage,
)
from .abc import (
    BrokerSubscriber,
)


class InMemoryBrokerSubscriber(BrokerSubscriber):
    """In Memory Broker Subscriber class."""

    _queue: Queue[BrokerMessage]

    def __init__(self, messages: Iterable[BrokerMessage], *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._queue = Queue()
        for message in messages:
            self._queue.put_nowait(message)

    async def receive(self) -> BrokerMessage:
        """Receive a new message.

        :return: A ``BrokerMessage`` instance.
        """
        return await self._queue.get()
