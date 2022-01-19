from typing import (
    AsyncIterator,
    NoReturn,
)

from ...messages import (
    BrokerMessage,
)
from ..abc import (
    BrokerSubscriber,
)
from .repositories import (
    BrokerSubscriberRepository,
)


class QueuedBrokerSubscriber(BrokerSubscriber):
    """TODO"""

    impl: BrokerSubscriber
    repository: BrokerSubscriberRepository

    def __init__(self, *args, impl: BrokerSubscriber, repository: BrokerSubscriberRepository, **kwargs):
        super().__init__(*args, **kwargs)

        if self.topics != impl.topics:
            raise Exception()

        self.impl = impl
        self.repository = repository

    async def _setup(self) -> None:
        await super()._setup()
        await self.repository.setup()
        await self.impl.setup()

    async def _destroy(self) -> None:
        await self.impl.destroy()
        await self.repository.destroy()
        await super()._destroy()

    def receive_all(self) -> AsyncIterator[BrokerMessage]:
        """TODO

        :return: TODO
        """
        return self.repository.dequeue_all()

    async def run(self) -> NoReturn:
        """TODO

        :return: TODO
        """

        while True:
            async for message in self.impl.receive_all():
                await self.repository.enqueue(message)
