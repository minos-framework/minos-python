from __future__ import (
    annotations,
)

import logging
from asyncio import (
    TimeoutError,
    sleep,
    wait_for,
)
from contextlib import (
    suppress,
)

from aiokafka import (
    AIOKafkaProducer,
)
from aiomisc import (
    CircuitBreaker,
)
from kafka.errors import (
    KafkaConnectionError,
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
    """Kafka Broker Publisher class."""

    def __init__(self, *args, broker_host: str, broker_port: int, **kwargs):
        super().__init__(*args, **kwargs)
        self.broker_host = broker_host
        self.broker_port = broker_port

        self._client = None

        self._circuit_breaker = CircuitBreaker(
            error_ratio=0.2, response_time=20, exceptions=[KafkaConnectionError], broken_time=5
        )

    @classmethod
    def _from_config(cls, config: MinosConfig, **kwargs) -> KafkaBrokerPublisher:
        kwargs["broker_host"] = config.broker.host
        kwargs["broker_port"] = config.broker.port
        # noinspection PyProtectedMember
        return cls(**config.broker.queue._asdict(), **kwargs)

    async def _setup(self) -> None:
        await super()._setup()
        await self._start_client()

    async def _destroy(self) -> None:
        await self._stop_client()
        await super()._destroy()

    async def _start_client(self) -> None:
        while True:
            with suppress(KafkaConnectionError):
                with self._circuit_breaker.context():
                    await self.client.start()
                    return
            await sleep(1)

    async def _stop_client(self):
        with suppress(TimeoutError):
            await wait_for(self._client.stop(), 0.5)

    async def _send(self, message: BrokerMessage) -> None:
        while True:
            with suppress(KafkaConnectionError):
                with self._circuit_breaker.context():
                    await self.client.send_and_wait(message.topic, message.avro_bytes)
                    return
            await sleep(1)

    @property
    def client(self) -> AIOKafkaProducer:
        """Get the client instance.

        :return: An ``AIOKafkaProducer`` instance.
        """
        if self._client is None:
            self._client = self._build_client()
        return self._client

    def _build_client(self) -> AIOKafkaProducer:
        return AIOKafkaProducer(bootstrap_servers=f"{self.broker_host}:{self.broker_port}")
