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
        """Send a message.

        :param message: The message to be sent.
        :return: This method does not return anything.
        """
