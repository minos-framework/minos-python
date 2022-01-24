from asyncio import (
    CancelledError,
    TimeoutError,
    create_task,
    wait_for,
)
from collections.abc import (
    Iterable,
)
from contextlib import (
    suppress,
)
from typing import (
    Any,
    Awaitable,
    NoReturn,
)

from minos.common import (
    MinosConfig,
)

from ...messages import (
    BrokerMessage,
)
from ..abc import (
    BrokerSubscriber,
    BrokerSubscriberBuilder,
)
from .repositories import (
    BrokerSubscriberRepository,
    BrokerSubscriberRepositoryBuilder,
)


class QueuedBrokerSubscriber(BrokerSubscriber):
    """Queued Broker Subscriber class."""

    impl: BrokerSubscriber
    repository: BrokerSubscriberRepository

    def __init__(self, impl: BrokerSubscriber, repository: BrokerSubscriberRepository, **kwargs):
        super().__init__(kwargs.pop("topics", impl.topics), **kwargs)
        if self.topics != impl.topics or self.topics != repository.topics:
            raise ValueError("The topics from the impl and repository must be equal")

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


class QueuedBrokerSubscriberBuilder(BrokerSubscriberBuilder):
    """TODO"""

    def __init__(
        self,
        *args,
        impl_builder: BrokerSubscriberBuilder,
        repository_builder: BrokerSubscriberRepositoryBuilder,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self._impl_builder = impl_builder
        self._repository_builder = repository_builder

    def with_config(self, config: MinosConfig) -> BrokerSubscriberBuilder:
        """TODO"""
        self._impl_builder.with_config(config)
        self._repository_builder.with_config(config)
        return super().with_config(config)

    def with_kwargs(self, kwargs: dict[str, Any]) -> BrokerSubscriberBuilder:
        """TODO"""
        self._impl_builder.with_kwargs(kwargs)
        self._repository_builder.with_kwargs(kwargs)
        return super().with_kwargs(kwargs)

    def with_topics(self, topics: Iterable[str]) -> BrokerSubscriberBuilder:
        """TODO"""
        topics = set(topics)
        self._impl_builder.with_topics(topics)
        self._repository_builder.with_topics(topics)
        return super().with_topics(topics)

    def build(self) -> BrokerSubscriber:
        """TODO"""
        impl = self._impl_builder.build()
        repository = self._repository_builder.build()
        return QueuedBrokerSubscriber(impl=impl, repository=repository, **self.kwargs)
