from abc import (
    ABC,
    abstractmethod,
)

from minos.common import (
    MinosSetup,
)

from ..messages import (
    BrokerMessage,
)


class BrokerPublisher(ABC, MinosSetup):
    """Broker Publisher class."""

    @abstractmethod
    async def send(self, message: BrokerMessage) -> None:
        """TODO

        :param message: TODO
        :return: TODO
        """
