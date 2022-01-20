from asyncio import (
    CancelledError,
    TimeoutError,
    create_task,
    wait_for,
)
from contextlib import (
    suppress,
)
from typing import (
    NoReturn,
)

from ...messages import (
    BrokerMessage,
)
from ..abc import (
    BrokerPublisher,
)
from .repositories import (
    BrokerPublisherRepository,
)


class QueuedBrokerPublisher(BrokerPublisher):
    """Queued Broker Publisher class."""

    impl: BrokerPublisher
    repository: BrokerPublisherRepository

    def __init__(self, impl: BrokerPublisher, repository: BrokerPublisherRepository, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.impl = impl
        self.repository = repository

        self._run_task = None

    async def _setup(self) -> None:
        await super()._setup()
        await self.repository.setup()
        await self.impl.setup()

        if self._run_task is None:
            self._run_task = create_task(self._run())

    async def _destroy(self) -> None:
        if self._run_task is not None:
            self._run_task.cancel()
            with suppress(TimeoutError, CancelledError):
                await wait_for(self._run_task, 0.5)
            self._run_task = None

        await self.impl.destroy()
        await self.repository.destroy()
        await super()._destroy()

    async def _run(self) -> NoReturn:
        async for message in self.repository:
            await self.impl.send(message)

    async def send(self, message: BrokerMessage) -> None:
        """Send method."""
        await self.repository.enqueue(message)
