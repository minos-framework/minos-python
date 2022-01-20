from ..messages import BrokerMessage
from .abc import BrokerSubscriber


class InMemoryBrokerSubscriber(BrokerSubscriber):
    """TODO"""

    async def receive(self) -> BrokerMessage:
        """TODO

        :return: TODO
        """
        raise NotImplementedError
