import logging
from asyncio import (
    Queue,
)

from ....messages import (
    BrokerMessage,
)
from .abc import (
    BrokerSubscriberRepository,
    BrokerSubscriberRepositoryBuilder,
)

logger = logging.getLogger(__name__)


class InMemoryBrokerSubscriberRepository(BrokerSubscriberRepository):
    """In Memory Broker Subscriber Repository class."""

    _queue: Queue[BrokerMessage]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._queue = Queue()

    async def enqueue(self, message: BrokerMessage) -> None:
        """Enqueue a new message.

        :param message: The ``BrokerMessage`` to be enqueued.
        :return: This method does not return anything.
        """
        logger.info(f"Enqueueing {message!r} message...")
        await self._queue.put(message)

    async def dequeue(self) -> BrokerMessage:
        """Dequeue a message from the queue.

        :return: The dequeued ``BrokerMessage``.
        """
        message = await self._queue.get()
        logger.info(f"Dequeuing {message!r} message...")
        return message


class InMemoryBrokerSubscriberRepositoryBuilder(BrokerSubscriberRepositoryBuilder):
    """In Memory Broker Subscriber Repository Builder class."""

    def build(self) -> BrokerSubscriberRepository:
        """Build the instance.

        :return: An ``InMemoryBrokerSubscriberRepository`` instance.
        """
        return InMemoryBrokerSubscriberRepository(**self.kwargs)
