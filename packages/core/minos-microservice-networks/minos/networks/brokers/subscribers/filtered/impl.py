from minos.common import (
    Builder,
)

from ...messages import (
    BrokerMessage,
)
from ..abc import (
    BrokerSubscriber,
)
from .validators import (
    BrokerSubscriberValidator,
)

_sentinel = object()


class FilteredBrokerSubscriber(BrokerSubscriber):
    """Filtered Broker Subscriber class."""

    impl: BrokerSubscriber
    validator: BrokerSubscriberValidator

    def __init__(self, impl: BrokerSubscriber, validator: BrokerSubscriberValidator, **kwargs):
        super().__init__(**(kwargs | {"topics": impl.topics}))
        self.impl = impl
        self.validator = validator

    async def _setup(self) -> None:
        await super()._setup()
        await self.validator.setup()
        await self.impl.setup()

    async def _destroy(self) -> None:
        await self.impl.destroy()
        await self.validator.destroy()
        await super()._destroy()

    async def _receive(self) -> BrokerMessage:
        message = _sentinel
        while message is _sentinel or not (await self.validator.is_valid(message)):
            message = await self.impl.receive()
        return message


FilteredBrokerSubscriber.set_builder(Builder)
