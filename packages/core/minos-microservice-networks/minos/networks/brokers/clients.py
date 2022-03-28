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
from uuid import (
    uuid4,
)

from minos.common import (
    Config,
    Inject,
    NotProvidedException,
    SetupMixin,
)

from ..exceptions import (
    MinosHandlerNotFoundEnoughEntriesException,
)
from .messages import (
    BrokerMessage,
)
from .publishers import (
    BrokerPublisher,
)
from .subscribers import (
    BrokerSubscriber,
    BrokerSubscriberBuilder,
)

logger = logging.getLogger(__name__)


class BrokerClient(SetupMixin):
    """Broker Client class."""

    def __init__(self, topic: str, publisher: BrokerPublisher, subscriber: BrokerSubscriber, **kwargs):
        super().__init__(**kwargs)

        self.topic = topic
        self.publisher = publisher
        self.subscriber = subscriber

    @classmethod
    def _from_config(cls, config: Config, **kwargs) -> BrokerClient:
        if "topic" not in kwargs:
            kwargs["topic"] = str(uuid4()).replace("-", "")

        kwargs["publisher"] = cls._get_publisher(**kwargs)

        kwargs["subscriber"] = cls._get_subscriber(config, **kwargs)

        return cls(**kwargs)

    # noinspection PyUnusedLocal
    @staticmethod
    @Inject()
    def _get_publisher(
        publisher: Optional[BrokerPublisher] = None,
        broker_publisher: Optional[BrokerPublisher] = None,
        **kwargs,
    ) -> BrokerPublisher:
        if publisher is None:
            publisher = broker_publisher
        if publisher is None:
            raise NotProvidedException(f"A {BrokerPublisher!r} object must be provided.")
        return publisher

    @staticmethod
    @Inject()
    def _get_subscriber(
        config: Config,
        topic: str,
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
                    subscriber_builder.copy()
                    .with_config(config)
                    .with_topics({topic})
                    .with_group_id(None)
                    .with_remove_topics_on_destroy(True)
                    .with_kwargs(kwargs)
                    .build()
                )

        if not isinstance(subscriber, BrokerSubscriber):
            raise NotProvidedException(f"A {BrokerSubscriber!r} or {BrokerSubscriberBuilder!r} must be provided.")

        return subscriber

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
        async for message in self.subscriber:
            result.append(message)

            if len(result) == count:
                break

        return result
