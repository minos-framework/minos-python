from __future__ import (
    annotations,
)

import logging
from asyncio import (
    Queue,
    create_task,
    gather,
)
from collections.abc import (
    Iterable,
)
from typing import (
    NoReturn,
    Optional,
)

from minos.common import (
    Config,
    Inject,
    NotProvidedException,
    SetupMixin,
)

from ..dispatchers import (
    BrokerDispatcher,
)
from ..subscribers import (
    BrokerSubscriber,
    BrokerSubscriberBuilder,
)

logger = logging.getLogger(__name__)


class BrokerHandler(SetupMixin):
    """Broker Handler class."""

    def __init__(
        self, dispatcher: BrokerDispatcher, subscriber: BrokerSubscriber, concurrency: int = 5, *args, **kwargs
    ):
        super().__init__(*args, **kwargs)

        self._dispatcher = dispatcher
        self._subscriber = subscriber

        self._queue = Queue(maxsize=1)
        self._consumers = list()
        self._concurrency = concurrency

    @classmethod
    def _from_config(cls, config: Config, **kwargs) -> BrokerHandler:
        dispatcher = cls._get_dispatcher(config, **kwargs)
        subscriber = cls._get_subscriber(config, topics=set(dispatcher.actions.keys()), **kwargs)

        return cls(dispatcher, subscriber, **kwargs)

    @staticmethod
    @Inject()
    def _get_dispatcher(
        config: Config,
        dispatcher: Optional[BrokerDispatcher] = None,
        broker_dispatcher: Optional[BrokerDispatcher] = None,
        **kwargs,
    ) -> BrokerDispatcher:
        if dispatcher is None:
            dispatcher = broker_dispatcher
        if dispatcher is None:
            dispatcher = BrokerDispatcher.from_config(config, **kwargs)
        return dispatcher

    @staticmethod
    @Inject()
    def _get_subscriber(
        config: Config,
        topics: Iterable[str],
        subscriber: Optional[BrokerSubscriber] = None,
        broker_subscriber: Optional[BrokerSubscriber] = None,
        subscriber_builder: Optional[BrokerSubscriberBuilder] = None,
        broker_subscriber_builder: Optional[BrokerSubscriberBuilder] = None,
        **kwargs,
    ) -> BrokerSubscriber:
        if not isinstance(subscriber, BrokerSubscriber):
            subscriber = broker_subscriber

        if not isinstance(subscriber, BrokerSubscriber):
            if not isinstance(subscriber_builder, BrokerSubscriberBuilder):
                subscriber_builder = broker_subscriber_builder

            if isinstance(subscriber_builder, BrokerSubscriberBuilder):
                subscriber = (
                    subscriber_builder.copy().with_config(config).with_topics(topics).with_kwargs(kwargs).build()
                )

        if not isinstance(subscriber, BrokerSubscriber):
            raise NotProvidedException(f"A {BrokerSubscriber!r} or {BrokerSubscriberBuilder!r} must be provided.")

        return subscriber

    async def _setup(self) -> None:
        await super()._setup()
        await self._dispatcher.setup()

        await self._create_consumers()

        await self._subscriber.setup()

    async def _destroy(self) -> None:
        await self._subscriber.destroy()

        await self._destroy_consumers()

        await self._dispatcher.destroy()
        await super()._destroy()

    async def run(self) -> NoReturn:
        """Run the message handling process.

        :return: This method does not return anything.
        """
        async for message in self._subscriber:
            await self._queue.put(message)

    async def _create_consumers(self):
        while len(self._consumers) < self._concurrency:
            self._consumers.append(create_task(self._consume()))

    async def _destroy_consumers(self):
        await self._queue.join()
        for consumer in self._consumers:
            consumer.cancel()
        await gather(*self._consumers, return_exceptions=True)
        self._consumers = list()

    async def _consume(self) -> None:
        while True:
            await self._consume_one()

    async def _consume_one(self) -> None:
        message = await self._queue.get()
        try:
            try:
                await self._dispatcher.dispatch(message)
            except Exception as exc:
                logger.warning(f"An exception was raised: {exc!r}")
        finally:
            self._queue.task_done()
