from asyncio import (
    CancelledError,
    PriorityQueue,
    create_task,
    gather,
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

        self._queue = PriorityQueue(maxsize=1)
        self._consumers = list()
        self._consumer_concurrency = 15

        self._run_task = None

    async def _setup(self) -> None:
        await super()._setup()
        await self.repository.setup()
        await self.impl.setup()
        await self._create_consumers()

        if self._run_task is None:
            self._run_task = create_task(self._run())

    async def _destroy(self) -> None:
        if self._run_task is not None:
            self._run_task.cancel()
            with suppress(CancelledError):
                await self._run_task
            self._run_task = None

        await self._destroy_consumers()
        await self.impl.destroy()
        await self.repository.destroy()
        await super()._destroy()

    async def _run(self) -> NoReturn:
        while True:
            async for message in self.impl:
                await self._queue.put(message)

    async def _create_consumers(self):
        while len(self._consumers) < self._consumer_concurrency:
            self._consumers.append(create_task(self._consume()))

    async def _destroy_consumers(self):
        for consumer in self._consumers:
            consumer.cancel()
        await gather(*self._consumers, return_exceptions=True)
        self._consumers = list()

        while not self._queue.empty():
            # FIXME
            message = self._queue.get_nowait()  # noqa

    async def _consume(self) -> None:
        while True:
            await self._consume_one()

    async def _consume_one(self) -> None:
        message = await self._queue.get()
        try:
            try:
                await self.repository.enqueue(message)
            except (CancelledError, Exception) as exc:
                # await self.submit_query(self._queries["update_not_processed"], (entry.id,))
                raise exc
            # await self.submit_query(self._queries["delete_processed"], (entry.id,))
        finally:
            self._queue.task_done()

    def receive(self) -> Awaitable[BrokerMessage]:
        """TODO

        :return: TODO
        """
        return self.repository.dequeue()
