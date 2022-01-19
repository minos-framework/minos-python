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

    def __aiter__(self) -> AsyncIterator[BrokerMessage]:
        return self

    async def __anext__(self) -> BrokerMessage:
        if self.already_destroyed:
            raise StopAsyncIteration
        return await self.dequeue()

    @abstractmethod
    async def dequeue(self) -> BrokerMessage:
        """Dequeue method."""
