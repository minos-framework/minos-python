from __future__ import (
    annotations,
)

from abc import (
    ABC,
    abstractmethod,
)

from minos.common import (
    BuildableMixin,
)

from ....messages import (
    BrokerMessage,
)


class BrokerSubscriberValidator(BuildableMixin, ABC):
    """Broker Subscriber Duplicate Detector class."""

    async def is_valid(self, message: BrokerMessage) -> bool:
        """Check if the given message is valid.

        :param message: The message to be checked.
        :return: ``True`` if it is valid or ``False`` otherwise.
        """
        return await self._is_valid(message)

    @abstractmethod
    async def _is_valid(self, message: BrokerMessage) -> bool:
        raise NotImplementedError
