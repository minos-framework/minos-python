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
    """Idempotent Broker Subscriber class."""

    impl: BrokerSubscriber
    duplicate_detector: BrokerSubscriberDuplicateDetector

    def __init__(self, impl: BrokerSubscriber, duplicate_detector: BrokerSubscriberDuplicateDetector, **kwargs):
        super().__init__(**(kwargs | {"topics": impl.topics}))
        self.impl = impl
        self.duplicate_detector = duplicate_detector

    async def _setup(self) -> None:
        await super()._setup()
        await self.duplicate_detector.setup()
        await self.impl.setup()

    async def _destroy(self) -> None:
        await self.impl.destroy()
        await self.duplicate_detector.destroy()
        await super()._destroy()

    async def _receive(self) -> BrokerMessage:
        message = _sentinel
        while message is _sentinel or not (await self.duplicate_detector.is_valid(message)):
            message = await self.impl.receive()
        return message
