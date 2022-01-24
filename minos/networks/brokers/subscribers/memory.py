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
    BrokerSubscriberBuilder,
)


class InMemoryBrokerSubscriber(BrokerSubscriber):
    """In Memory Broker Subscriber class."""

    _queue: Queue[BrokerMessage]

    def __init__(self, topics: Iterable[str], messages: Iterable[BrokerMessage] = tuple(), **kwargs):
        super().__init__(topics, **kwargs)
        self._queue = Queue()
        for message in messages:
            self._queue.put_nowait(message)

    def add_message(self, message: BrokerMessage) -> None:
        """Add a message to the subscriber.

        :param message: The message to be added.
        :return: This method does not return anything.
        """
        self._queue.put_nowait(message)

    async def receive(self) -> BrokerMessage:
        """Receive a new message.

        :return: A ``BrokerMessage`` instance.
        """
        return await self._queue.get()


class InMemoryBrokerSubscriberBuilder(BrokerSubscriberBuilder):
    """TODO"""

    def build(self) -> BrokerSubscriber:
        """TODO"""
        return InMemoryBrokerSubscriber(**self.kwargs)
