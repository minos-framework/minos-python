from __future__ import (
    annotations,
)

from abc import (
    ABC,
    abstractmethod,
)
from uuid import (
    UUID,
)

from minos.common import (
    MinosSetup,
)

from ....messages import (
    BrokerMessage,
)


class BrokerSubscriberDuplicateDetector(ABC, MinosSetup):
    """TODO"""

    async def is_valid(self, message: BrokerMessage) -> bool:
        """TODO

        :param message: TODO
        :return: TODO
        """
        return await self._is_valid(message.topic, message.identifier)

    @abstractmethod
    async def _is_valid(self, topic: str, uuid: UUID) -> bool:
        raise NotImplementedError
