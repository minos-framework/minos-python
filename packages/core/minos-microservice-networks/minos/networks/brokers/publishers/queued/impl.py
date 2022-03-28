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

from minos.common import (
    Builder,
)

from ...messages import (
    BrokerMessage,
)
from ..abc import (
    BrokerPublisher,
)
from .queues import (
    BrokerPublisherQueue,
)


class QueuedBrokerPublisher(BrokerPublisher):
    """Queued Broker Publisher class."""

    impl: BrokerPublisher
    queue: BrokerPublisherQueue

    def __init__(self, impl: BrokerPublisher, queue: BrokerPublisherQueue, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.impl = impl
        self.queue = queue

        self._run_task = None

    async def _setup(self) -> None:
        await super()._setup()
        await self.queue.setup()
        await self.impl.setup()
        await self._start_task()

    async def _destroy(self) -> None:
        await self._stop_task()
        await self.impl.destroy()
        await self.queue.destroy()
        await super()._destroy()

    async def _start_task(self):
        if self._run_task is None:
            self._run_task = create_task(self._run())

    async def _stop_task(self):
        if self._run_task is not None:
            self._run_task.cancel()
            with suppress(TimeoutError, CancelledError):
                await wait_for(self._run_task, 0.5)
            self._run_task = None

    async def _run(self) -> NoReturn:
        async for message in self.queue:
            await self.impl.send(message)

    async def _send(self, message: BrokerMessage) -> None:
        await self.queue.enqueue(message)


QueuedBrokerPublisher.set_builder(Builder)
