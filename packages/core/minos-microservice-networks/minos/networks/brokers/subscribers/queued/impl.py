from __future__ import (
    annotations,
)

import warnings
from asyncio import (
    CancelledError,
    TimeoutError,
    create_task,
    wait_for,
)
from collections.abc import (
    Awaitable,
    Iterable,
)
from contextlib import (
    suppress,
)
from typing import (
    Any,
    NoReturn,
    Optional,
)

from minos.common import (
    Config,
)

from ...messages import (
    BrokerMessage,
)
from ..abc import (
    BrokerSubscriber,
    BrokerSubscriberBuilder,
)
from .queues import (
    BrokerSubscriberQueue,
    BrokerSubscriberQueueBuilder,
)


class QueuedBrokerSubscriber(BrokerSubscriber):
    """Queued Broker Subscriber class."""

    impl: BrokerSubscriber
    queue: BrokerSubscriberQueue

    def __init__(self, impl: BrokerSubscriber, queue: BrokerSubscriberQueue, **kwargs):
        super().__init__(kwargs.pop("topics", impl.topics), **kwargs)
        if self.topics != impl.topics or self.topics != queue.topics:
            raise ValueError("The topics from the impl and queue must be equal")

        self.impl = impl
        self.queue = queue

        self._run_task = None

    async def _setup(self) -> None:
        await super()._setup()
        await self.queue.setup()
        await self.impl.setup()
        await self._start_run()

    async def _destroy(self) -> None:
        await self._stop_run()
        await self.impl.destroy()
        await self.queue.destroy()
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
            await self.queue.enqueue(message)

    def _receive(self) -> Awaitable[BrokerMessage]:
        return self.queue.dequeue()


class QueuedBrokerSubscriberBuilder(BrokerSubscriberBuilder[QueuedBrokerSubscriber]):
    """Queued Broker Subscriber Publisher class."""

    def __init__(
        self, *args, impl_builder: BrokerSubscriberBuilder, queue_builder: BrokerSubscriberQueueBuilder, **kwargs
    ):
        warnings.warn(f"{type(self)!r} has been deprecated. Use {BrokerSubscriberBuilder} instead.", DeprecationWarning)

        super().__init__(*args, **kwargs)
        self.impl_builder = impl_builder
        self.queue_builder = queue_builder

    def with_config(self, config: Config) -> QueuedBrokerSubscriberBuilder:
        """Set config.

        :param config: The config to be set.
        :return: This method return the builder instance.
        """
        self.impl_builder.with_config(config)
        self.queue_builder.with_config(config)
        return self

    def with_kwargs(self, kwargs: dict[str, Any]) -> QueuedBrokerSubscriberBuilder:
        """Set kwargs.

        :param kwargs: The kwargs to be set.
        :return: This method return the builder instance.
        """
        self.impl_builder.with_kwargs(kwargs)
        self.queue_builder.with_kwargs(kwargs)
        return self

    def with_topics(self, topics: Iterable[str]) -> QueuedBrokerSubscriberBuilder:
        """Set topics.

        :param topics: The topics to be set.
        :return: This method return the builder instance.
        """
        topics = set(topics)
        self.impl_builder.with_topics(topics)
        self.queue_builder.with_topics(topics)
        return self

    def with_group_id(self, group_id: Optional[str]) -> QueuedBrokerSubscriberBuilder:
        """Set group_id.

        :param group_id: The group_id to be set.
        :return: This method return the builder instance.
        """
        self.impl_builder.with_group_id(group_id)
        return self

    def with_remove_topics_on_destroy(self, remove_topics_on_destroy: bool) -> QueuedBrokerSubscriberBuilder:
        """Set remove_topics_on_destroy.

        :param remove_topics_on_destroy: The remove_topics_on_destroy flag to be set.
        :return: This method return the builder instance.
        """
        self.impl_builder.with_remove_topics_on_destroy(remove_topics_on_destroy)
        return self

    def build(self) -> QueuedBrokerSubscriber:
        """Build the instance.

        :return: A ``QueuedBrokerSubscriber`` instance.
        """
        impl = self.impl_builder.build()
        queue = self.queue_builder.build()
        return QueuedBrokerSubscriber(impl=impl, queue=queue, **self.kwargs)
