import logging
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

logger = logging.getLogger(__name__)


class BrokerPublisher(ABC, MinosSetup):
    """Broker Publisher class."""

    async def send(self, message: BrokerMessage) -> None:
        """Send a message.

        :param message: The message to be sent.
        :return: This method does not return anything.
        """
        logger.debug(f"Sending {message!r} message...")
        await self._send(message)

    @abstractmethod
    async def _send(self, message: BrokerMessage) -> None:
        raise NotImplementedError
