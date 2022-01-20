from abc import (
    ABC,
    abstractmethod,
)
from collections.abc import AsyncIterator

from minos.common import MinosSetup

from ..messages import BrokerMessage


class BrokerSubscriber(ABC, MinosSetup):
    """TODO"""

    def __init__(self, topics: set[str], *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._topics = topics

    @property
    def topics(self) -> set[str]:
        """Topics getter.

        :return: A list of string values.
        """
        return self._topics

    def __aiter__(self) -> AsyncIterator[BrokerMessage]:
        return self

    async def __anext__(self) -> BrokerMessage:
        if self.already_destroyed:
            raise StopAsyncIteration
        return await self.receive()

    @abstractmethod
    async def receive(self) -> BrokerMessage:
        """TODO

        :return: TODO
        """
