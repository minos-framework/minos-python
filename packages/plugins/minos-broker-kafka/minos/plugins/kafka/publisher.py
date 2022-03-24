from __future__ import (
    annotations,
)

import logging
from asyncio import (
    TimeoutError,
    wait_for,
)
from contextlib import (
    suppress,
)
from functools import (
    partial,
)
from typing import (
    Optional,
)

from aiokafka import (
    AIOKafkaProducer,
)

from minos.common import (
    Config,
)
from minos.networks import (
    BrokerMessage,
    BrokerPublisher,
    InMemoryBrokerPublisherQueue,
    PostgreSqlBrokerPublisherQueue,
    QueuedBrokerPublisher,
)

from .mixins import (
    KafkaCircuitBreakerMixin,
)

logger = logging.getLogger(__name__)


class PostgreSqlQueuedKafkaBrokerPublisher(QueuedBrokerPublisher):
    """PostgreSql Queued Kafka Broker Publisher class."""

    @classmethod
    def _from_config(cls, config: Config, **kwargs) -> PostgreSqlQueuedKafkaBrokerPublisher:
        impl = KafkaBrokerPublisher.from_config(config, **kwargs)
        queue = PostgreSqlBrokerPublisherQueue.from_config(config, **kwargs)
        return cls(impl, queue, **kwargs)


class InMemoryQueuedKafkaBrokerPublisher(QueuedBrokerPublisher):
    """In Memory Queued Kafka Broker Publisher class."""

    @classmethod
    def _from_config(cls, config: Config, **kwargs) -> InMemoryQueuedKafkaBrokerPublisher:
        impl = KafkaBrokerPublisher.from_config(config, **kwargs)
        queue = InMemoryBrokerPublisherQueue.from_config(config, **kwargs)
        return cls(impl, queue, **kwargs)


class KafkaBrokerPublisher(BrokerPublisher, KafkaCircuitBreakerMixin):
    """Kafka Broker Publisher class."""

    def __init__(self, *args, broker_host: Optional[str] = None, broker_port: Optional[int] = None, **kwargs):
        super().__init__(*args, **kwargs)

        if broker_host is None:
            broker_host = "localhost"

        if broker_port is None:
            broker_port = 9092

        self.broker_host = broker_host
        self.broker_port = broker_port

        self._client = None

    @classmethod
    def _from_config(cls, config: Config, **kwargs) -> KafkaBrokerPublisher:
        broker_config = config.get_interface_by_name("broker")
        common_config = broker_config.get("common", dict())
        kwargs["broker_host"] = common_config.get("host")
        kwargs["broker_port"] = common_config.get("port")
        return cls(**kwargs)

    async def _setup(self) -> None:
        await super()._setup()
        await self._start_client()

    async def _destroy(self) -> None:
        await self._stop_client()
        await super()._destroy()

    async def _start_client(self) -> None:
        # noinspection PyBroadException
        try:
            await self.with_circuit_breaker(self.client.start)
        except Exception as exc:
            await self._stop_client()
            raise exc

    async def _stop_client(self):
        with suppress(TimeoutError):
            await wait_for(self._client.stop(), 0.5)

    async def _send(self, message: BrokerMessage) -> None:
        fn = partial(self.client.send_and_wait, message.topic, message.avro_bytes)
        await self.with_circuit_breaker(fn)

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
