from ...messages import (
    BrokerMessage,
)
from ..abc import (
    BrokerPublisher,
)
from .repositories import (
    BrokerPublisherRepository,
)


class QueuedBrokerPublisher(BrokerPublisher):
    """Queued Broker Publisher class."""

    impl: BrokerPublisher
    repository: BrokerPublisherRepository

    def __init__(self, impl: BrokerPublisher, repository: BrokerPublisherRepository, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.impl = impl
        self.repository = repository

    async def send(self, message: BrokerMessage) -> None:
        """Send method."""
        await self.repository.enqueue(message)

    async def run(self) -> None:
        """Run method."""
        while True:
            message = await self.repository.dequeue()
            await self.impl.send(message)
