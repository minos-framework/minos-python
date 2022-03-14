from ...messages import (
    BrokerMessage,
)
from ..abc import (
    BrokerSubscriber,
)
from .detectors import (
    BrokerSubscriberDuplicateDetector,
)

_sentinel = object()


class IdempotentBrokerSubscriber(BrokerSubscriber):
    """TODO"""

    impl: BrokerSubscriber
    duplicates_detector: BrokerSubscriberDuplicateDetector

    async def _setup(self) -> None:
        await super()._setup()
        await self.duplicates_detector.setup()
        await self.impl.setup()

    async def _destroy(self) -> None:
        await self.impl.destroy()
        await self.duplicates_detector.destroy()
        await super()._destroy()

    async def _receive(self) -> BrokerMessage:
        message = _sentinel
        while message is _sentinel or not self.duplicates_detector.is_valid(message):
            message = await self.impl.receive()
        return message
