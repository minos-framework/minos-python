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

    async def _setup(self) -> None:
        await super()._setup()
        await self.repository.setup()
        await self.impl.setup()

    async def _destroy(self) -> None:
        await self.impl.destroy()
        await self.repository.destroy()
        await super()._destroy()

    async def send(self, message: BrokerMessage) -> None:
        """Send method."""
        await self.repository.enqueue(message)

    async def run(self) -> None:
        """Run method."""
        while True:
            async for message in self.repository.dequeue_all():
                await self.impl.send(message)
