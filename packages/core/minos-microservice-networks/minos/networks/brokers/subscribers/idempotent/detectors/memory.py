from uuid import (
    UUID,
)

from .abc import (
    BrokerSubscriberDuplicateDetector,
)


class InMemoryBrokerSubscriberDuplicateDetector(BrokerSubscriberDuplicateDetector):
    """TODO"""

    async def _is_valid(self, topic: str, uuid: UUID) -> bool:
        raise NotImplementedError
