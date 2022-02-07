from __future__ import (
    annotations,
)

import logging
from asyncio import (
    TimeoutError,
    sleep,
    wait_for,
)
from collections.abc import (
    Awaitable,
    Callable,
)
from contextlib import (
    suppress,
)
from functools import (
    partial,
)
from typing import (
    Union,
)

from aiokafka import (
    AIOKafkaProducer,
)
from aiomisc import (
    CircuitBreaker,
)
from aiomisc.circuit_breaker import (
    CircuitBroken,
)
from kafka.errors import (
    KafkaError,
)

from minos.common import (
    MinosConfig,
)
from minos.networks import (
    BrokerMessage,
    BrokerPublisher,
    InMemoryBrokerPublisherQueue,
    PostgreSqlBrokerPublisherQueue,
    QueuedBrokerPublisher,
)

logger = logging.getLogger(__name__)


class PostgreSqlQueuedKafkaBrokerPublisher(QueuedBrokerPublisher):
    """PostgreSql Queued Kafka Broker Publisher class."""

    @classmethod
    def _from_config(cls, config: MinosConfig, **kwargs) -> PostgreSqlQueuedKafkaBrokerPublisher:
        impl = KafkaBrokerPublisher.from_config(config, **kwargs)
        queue = PostgreSqlBrokerPublisherQueue.from_config(config, **kwargs)
        return cls(impl, queue, **kwargs)


class InMemoryQueuedKafkaBrokerPublisher(QueuedBrokerPublisher):
    """In Memory Queued Kafka Broker Publisher class."""

    @classmethod
    def _from_config(cls, config: MinosConfig, **kwargs) -> InMemoryQueuedKafkaBrokerPublisher:
        impl = KafkaBrokerPublisher.from_config(config, **kwargs)
        queue = InMemoryBrokerPublisherQueue.from_config(config, **kwargs)
        return cls(impl, queue, **kwargs)


class KafkaBrokerPublisher(BrokerPublisher):
    """Kafka Broker Publisher class."""

    def __init__(
        self, *args, broker_host: str, broker_port: int, circuit_breaker_time: Union[int, float] = 3, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.broker_host = broker_host
        self.broker_port = broker_port

        self._client = None

        self._circuit_breaker = CircuitBreaker(
            error_ratio=0.2, response_time=circuit_breaker_time, exceptions=[KafkaError]
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
        # noinspection PyBroadException
        try:
            await self._with_circuit_breaker(self.client.start)
        except Exception as exc:
            await self._stop_client()
            raise exc

    async def _stop_client(self):
        with suppress(TimeoutError):
            await wait_for(self._client.stop(), 0.5)

    async def _send(self, message: BrokerMessage) -> None:
        func = partial(self.client.send_and_wait, message.topic, message.avro_bytes)
        await self._with_circuit_breaker(func)

    async def _with_circuit_breaker(self, future_func: Callable[[], Awaitable[None]]) -> None:
        while True:
            try:
                with self._circuit_breaker.context():
                    return await future_func()
            except CircuitBroken:
                await sleep(self._circuit_breaker_timeout)
            except KafkaError as exc:
                logger.warning(f"A kafka exception was raised: {exc!r}")
                await sleep(self._exception_timeout)

    @property
    def _circuit_breaker_timeout(self) -> float:
        return self.circuit_breaker.response_time * 0.5

    @property
    def _exception_timeout(self) -> float:
        return self.circuit_breaker.response_time * 0.05

    @property
    def circuit_breaker(self) -> CircuitBreaker:
        """Get the circuit breaker.

        :return: A ``CircuitBreaker`` instance.
        """
        return self._circuit_breaker

    @property
    def client(self) -> AIOKafkaProducer:
        """Get the client instance.

        :return: An ``AIOKafkaProducer`` instance.
        """
        if self._client is None:
            self._client = self._build_client()
        return self._client

    def _build_client(self) -> AIOKafkaProducer:
        return AIOKafkaProducer(bootstrap_servers=self._bootstrap_servers)

    @property
    def _bootstrap_servers(self):
        return f"{self.broker_host}:{self.broker_port}"
