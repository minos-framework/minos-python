from abc import (
    ABC,
    abstractmethod,
)

from minos.common import (
    MinosSetup,
)

from ....messages import (
    BrokerMessage,
)


class BrokerPublisherRepository(ABC, MinosSetup):
    """Broker Publisher Repository class."""

    @abstractmethod
    async def enqueue(self, message: BrokerMessage) -> None:
        """Enqueue method."""

    @abstractmethod
    async def dequeue(self) -> BrokerMessage:
        """Dequeue method."""
