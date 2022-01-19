from collections.abc import (
    AsyncIterator,
)

from ..messages import (
    BrokerMessage,
)
from .abc import (
    BrokerSubscriber,
)


class InMemoryBrokerSubscriber(BrokerSubscriber):
    """TODO"""

    async def receive_all(self) -> AsyncIterator[BrokerMessage]:
        """TODO

        :return: TODO
        """
        raise NotImplementedError
