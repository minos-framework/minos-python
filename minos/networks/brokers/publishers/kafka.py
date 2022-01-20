from __future__ import (
    annotations,
)

import logging
from asyncio import (
    CancelledError,
    Queue,
    create_task,
    gather,
)

from aiokafka import (
    AIOKafkaProducer,
)
from cached_property import (
    cached_property,
)

from minos.common import (
    MinosConfig,
)

from ..messages import (
    BrokerMessage,
)
from .abc import (
    BrokerPublisher,
)

logger = logging.getLogger(__name__)


class KafkaBrokerPublisher(BrokerPublisher):
    """TODO"""

    def __init__(self, *args, broker_host: str, broker_port: int, concurrency: int = 4, **kwargs):
        super().__init__(*args, **kwargs)
        self.broker_host = broker_host
        self.broker_port = broker_port

        self._queue = Queue(maxsize=1)
        self._consumers = list()
        self._concurrency = concurrency

    @classmethod
    def _from_config(cls, config: MinosConfig, **kwargs) -> KafkaBrokerPublisher:
        kwargs["broker_host"] = config.broker.host
        kwargs["broker_port"] = config.broker.port
        # noinspection PyProtectedMember
        return cls(**config.broker.queue._asdict(), **kwargs)

    async def _setup(self) -> None:
        await super()._setup()
        await self.client.start()
        await self._create_consumers()

    async def _destroy(self) -> None:
        await self._destroy_consumers()
        await self.client.stop()
        await super()._destroy()

    async def send(self, message: BrokerMessage) -> None:
        """TODO

        :param message: TODO
        :return: TODO
        """
        logger.info(f"Producing {message!r} message...")
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

        while not self._queue.empty():
            # TODO
            message = self._queue.get_nowait()  # noqa

    async def _consume(self) -> None:
        while True:
            await self._consume_one()

    async def _consume_one(self) -> None:
        message = await self._queue.get()
        try:
            try:
                await self._send(message)
            except (CancelledError, Exception) as exc:
                # TODO
                raise exc
        finally:
            self._queue.task_done()

    async def _send(self, message: BrokerMessage) -> None:
        await self.client.send_and_wait(message.topic, message.avro_bytes)

    @cached_property
    def client(self) -> AIOKafkaProducer:
        """Get the client instance.

        :return: An ``AIOKafkaProducer`` instance.
        """
        return AIOKafkaProducer(bootstrap_servers=f"{self.broker_host}:{self.broker_port}")
