from abc import (
    ABC,
    abstractmethod,
)
from collections.abc import (
    AsyncIterator,
)

from minos.common import (
    MinosSetup,
)

from ....messages import (
    BrokerMessage,
)


class BrokerSubscriberRepository(ABC, MinosSetup):
    """TODO"""

    @abstractmethod
    async def enqueue(self, message: BrokerMessage) -> None:
        """TODO

        :param message: TODO
        :return: TODO
        """

    async def dequeue(self) -> BrokerMessage:
        """Dequeue method."""
        return await self.dequeue_all().__anext__()

    @abstractmethod
    def dequeue_all(self) -> AsyncIterator[BrokerMessage]:
        """Dequeue all method."""
