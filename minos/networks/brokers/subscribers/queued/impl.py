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
    """Queued Broker Subscriber class."""

    impl: BrokerSubscriber
    repository: BrokerSubscriberRepository

    def __init__(self, impl: BrokerSubscriber, repository: BrokerSubscriberRepository, **kwargs):
        super().__init__(kwargs.pop("topics", impl.topics), **kwargs)
        if self.topics or impl.topics or self.topics != repository.topics:
            raise ValueError(
                f"The topics from the impl and repository must be equal: {impl.topics!r} != {repository.topics!r}"
            )

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
            with suppress(TimeoutError, CancelledError):
                await wait_for(self._run_task, 0.5)
            self._run_task = None

    async def _run(self) -> NoReturn:
        async for message in self.impl:
            await self.repository.enqueue(message)

    def receive(self) -> Awaitable[BrokerMessage]:
        """Receive a new message.

        :return: A ``BrokerMessage`` instance.
        """
        return self.repository.dequeue()
