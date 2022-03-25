from __future__ import (
    annotations,
)

import logging
from asyncio import (
    CancelledError,
    Queue,
    TimeoutError,
    create_task,
    gather,
    wait_for,
)
from collections.abc import (
    Iterable,
)
from contextlib import (
    suppress,
)
from typing import (
    NoReturn,
    Optional,
)

from aio_pika import (
    connect,
)

from minos.networks import (
    BrokerMessage,
    BrokerSubscriber,
    BrokerSubscriberBuilder,
)

from .common import (
    RabbitMQBrokerBuilderMixin,
)

logger = logging.getLogger(__name__)


class RabbitMQBrokerSubscriber(BrokerSubscriber):
    """RabbitMQ Broker Subscriber class."""

    def __init__(
        self,
        topics: Iterable[str],
        host: Optional[str] = None,
        port: Optional[int] = None,
        **kwargs,
    ):
        super().__init__(topics, **kwargs)

        if host is None:
            host = "localhost"
        if port is None:
            port = 5672

        self.host = host
        self.port = port

        self.connection = None

        self._run_task = None
        self._queue: Queue[bytes] = Queue(maxsize=1)

    async def _setup(self) -> None:
        await super()._setup()
        self.connection = await connect(f"amqp://guest:guest@{self.host}:{self.port}/")
        await self._start_task()

    async def _destroy(self) -> None:
        await self._stop_task()
        await self.connection.close()

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
        await gather(*(self._run_one(topic) for topic in self.topics))

    async def _run_one(self, topic: str) -> None:
        channel = await self.connection.channel()
        queue = await channel.declare_queue(topic)
        async for message in queue.iterator():
            await self._queue.put(message.body)

    async def _receive(self) -> BrokerMessage:
        bytes_ = await self._queue.get()
        message = BrokerMessage.from_avro_bytes(bytes_)
        return message


class RabbitMQBrokerSubscriberBuilder(BrokerSubscriberBuilder[RabbitMQBrokerSubscriber], RabbitMQBrokerBuilderMixin):
    """RabbitMQ Broker Subscriber Builder class."""


RabbitMQBrokerSubscriber.set_builder(RabbitMQBrokerSubscriberBuilder)
