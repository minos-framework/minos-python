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
    Optional,
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

    def _receive(self) -> Awaitable[BrokerMessage]:
        return self.repository.dequeue()


class QueuedBrokerSubscriberBuilder(BrokerSubscriberBuilder):
    """Queued Broker Subscriber Publisher class."""

    def __init__(
        self,
        *args,
        impl_builder: BrokerSubscriberBuilder,
        repository_builder: BrokerSubscriberRepositoryBuilder,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.impl_builder = impl_builder
        self.repository_builder = repository_builder

    def with_config(self, config: MinosConfig) -> BrokerSubscriberBuilder:
        """Set config.

        :param config: The config to be set.
        :return: This method return the builder instance.
        """
        self.impl_builder.with_config(config)
        self.repository_builder.with_config(config)
        return super().with_config(config)

    def with_kwargs(self, kwargs: dict[str, Any]) -> BrokerSubscriberBuilder:
        """Set kwargs.

        :param kwargs: The kwargs to be set.
        :return: This method return the builder instance.
        """
        self.impl_builder.with_kwargs(kwargs)
        self.repository_builder.with_kwargs(kwargs)
        return super().with_kwargs(kwargs)

    def with_topics(self, topics: Iterable[str]) -> BrokerSubscriberBuilder:
        """Set topics.

        :param topics: The topics to be set.
        :return: This method return the builder instance.
        """
        topics = set(topics)
        self.impl_builder.with_topics(topics)
        self.repository_builder.with_topics(topics)
        return super().with_topics(topics)

    def with_group_id(self, group_id: Optional[str]) -> BrokerSubscriberBuilder:
        """Set group_id.

        :param group_id: The group_id to be set.
        :return: This method return the builder instance.
        """
        self.impl_builder.with_group_id(group_id)
        return super().with_group_id(group_id)

    def with_remove_topics_on_destroy(self, remove_topics_on_destroy: bool) -> BrokerSubscriberBuilder:
        """Set remove_topics_on_destroy.

        :param remove_topics_on_destroy: The remove_topics_on_destroy flag to be set.
        :return: This method return the builder instance.
        """
        self.impl_builder.with_remove_topics_on_destroy(remove_topics_on_destroy)
        return super().with_remove_topics_on_destroy(remove_topics_on_destroy)

    def build(self) -> BrokerSubscriber:
        """Build the instance.

        :return: A ``QueuedBrokerSubscriber`` instance.
        """
        impl = self.impl_builder.build()
        repository = self.repository_builder.build()
        return QueuedBrokerSubscriber(impl=impl, repository=repository, **self.kwargs)
