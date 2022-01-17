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

    async def send(self, message: BrokerMessage) -> None:
        """Send method."""
        await self.repository.enqueue(message)

    async def run(self) -> None:
        """Run method."""
        while True:
            message = await self.repository.dequeue()
            await self.impl.send(message)
