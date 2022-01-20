from asyncio import (
    CancelledError,
    create_task,
)
from contextlib import (
    suppress,
)
from typing import (
    Awaitable,
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

    def __init__(self, impl: BrokerSubscriber, repository: BrokerSubscriberRepository, *args, **kwargs):
        super().__init__(*args, **kwargs)

        if self.topics != impl.topics:
            raise Exception()

        self.impl = impl
        self.repository = repository

        self._run_task = None

    async def _setup(self) -> None:
        await super()._setup()
        await self.repository.setup()
        await self.impl.setup()
        await self._start_run()

    async def _destroy(self) -> None:
        await self._stop_run()
        await self.impl.destroy()
        await self.repository.destroy()
        await super()._destroy()

    async def _start_run(self):
        if self._run_task is None:
            self._run_task = create_task(self._run())

    async def _stop_run(self):
        if self._run_task is not None:
            self._run_task.cancel()
            with suppress(CancelledError):
                await self._run_task
            self._run_task = None

    async def _run(self) -> NoReturn:
        async for message in self.impl:
            await self.repository.enqueue(message)

    def receive(self) -> Awaitable[BrokerMessage]:
        """TODO

        :return: TODO
        """
        return self.repository.dequeue()
