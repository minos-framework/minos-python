from __future__ import (
    annotations,
)

import logging
from asyncio import (
    TimeoutError,
    wait_for,
)
from collections.abc import (
    AsyncIterator,
)
from typing import (
    Optional,
)

from dependency_injector.wiring import (
    Provide,
    inject,
)

from minos.common import (
    MinosConfig,
    MinosSetup,
    NotProvidedException,
)

from ...exceptions import (
    MinosHandlerNotFoundEnoughEntriesException,
)
from ..messages import (
    BrokerMessage,
)
from ..publishers import (
    BrokerPublisher,
)
from ..subscribers import (
    BrokerSubscriber,
)

logger = logging.getLogger(__name__)


class DynamicBroker(MinosSetup):
    """Dynamic Broker class."""

    def __init__(self, topic: str, publisher: BrokerPublisher, subscriber: BrokerSubscriber, **kwargs):
        super().__init__(**kwargs)

        self.topic = topic
        self.publisher = publisher
        self.subscriber = subscriber

    @classmethod
    def _from_config(cls, config: MinosConfig, **kwargs) -> DynamicBroker:
        from ..subscribers import (
            InMemoryQueuedKafkaBrokerSubscriber,
        )

        kwargs["publisher"] = cls._get_publisher(**kwargs)
        kwargs["subscriber"] = InMemoryQueuedKafkaBrokerSubscriber.from_config(
            config, topics={kwargs["topic"]}, group_id=kwargs["topic"]
        )
        # noinspection PyProtectedMember
        return cls(**kwargs)

    # noinspection PyUnusedLocal
    @staticmethod
    @inject
    def _get_publisher(
        publisher: Optional[BrokerPublisher] = None,
        broker_publisher: BrokerPublisher = Provide["broker_publisher"],
        **kwargs,
    ) -> BrokerPublisher:
        if publisher is None:
            publisher = broker_publisher
        if publisher is None or isinstance(publisher, Provide):
            raise NotProvidedException(f"A {BrokerPublisher!r} object must be provided.")
        return publisher

    async def _setup(self) -> None:
        await super()._setup()
        await self.subscriber.setup()

    async def _destroy(self) -> None:
        await self.subscriber.destroy()
        await super()._destroy()

    # noinspection PyUnusedLocal
    async def send(self, message: BrokerMessage) -> None:
        """Send a ``BrokerMessage``.

        :param message: The message to be sent.
        :return: This method does not return anything.
        """
        message.set_reply_topic(self.topic)
        await self.publisher.send(message)

    async def receive(self, *args, **kwargs) -> BrokerMessage:
        """Get one handler entry from the given topics.

        :param args: Additional positional parameters to be passed to receive_many.
        :param kwargs: Additional named parameters to be passed to receive_many.
        :return: A ``HandlerEntry`` instance.
        """
        return await self.receive_many(*args, **(kwargs | {"count": 1})).__anext__()

    async def receive_many(self, count: int, timeout: float = 60, **kwargs) -> AsyncIterator[BrokerMessage]:
        """Get multiple handler entries from the given topics.

        :param timeout: Maximum time in seconds to wait for messages.
        :param count: Number of entries to be collected.
        :return: A list of ``HandlerEntry`` instances.
        """
        try:
            messages = await wait_for(self._get_many(count, **kwargs), timeout=timeout)
        except TimeoutError:
            raise MinosHandlerNotFoundEnoughEntriesException(
                f"Timeout exceeded while trying to fetch {count!r} entries from {self.topic!r}."
            )

        for message in messages:
            logger.info(f"Dispatching '{message!s}'...")
            yield message

    async def _get_many(self, count, *args, **kwargs) -> list[BrokerMessage]:
        result = list()
        async for message in self.subscriber.receive_all():
            result.append(message)

            if len(result) == count:
                break

        return result
