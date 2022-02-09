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
    Iterable,
)
from contextlib import (
    suppress,
)
from functools import (
    partial,
)
from inspect import (
    isawaitable,
)
from typing import (
    Optional,
    TypeVar,
    Union,
)

from aiokafka import (
    AIOKafkaConsumer,
)
from aiomisc import (
    CircuitBreaker,
    CircuitBroken,
)
from cached_property import (
    cached_property,
)
from kafka import (
    KafkaAdminClient,
)
from kafka.admin import (
    NewTopic,
)
from kafka.errors import (
    KafkaError,
    TopicAlreadyExistsError,
)

from minos.common import (
    MinosConfig,
)
from minos.networks import (
    BrokerMessage,
    BrokerSubscriber,
    BrokerSubscriberBuilder,
    InMemoryBrokerSubscriberQueueBuilder,
    PostgreSqlBrokerSubscriberQueueBuilder,
    QueuedBrokerSubscriberBuilder,
)

logger = logging.getLogger(__name__)
R = TypeVar("R")


class KafkaBrokerSubscriberBuilder(BrokerSubscriberBuilder):
    """Kafka Broker Subscriber Builder class."""

    def with_config(self, config: MinosConfig) -> BrokerSubscriberBuilder:
        """Set config.

        :param config: The config to be set.
        :return: This method return the builder instance.
        """
        self.kwargs |= {
            "group_id": config.service.name,
            "broker_host": config.broker.host,
            "broker_port": config.broker.port,
        }
        return self

    def build(self) -> BrokerSubscriber:
        """Build the instance.

        :return: A ``KafkaBrokerSubscriber`` instance.
        """
        return KafkaBrokerSubscriber(**self.kwargs)


class PostgreSqlQueuedKafkaBrokerSubscriberBuilder(QueuedBrokerSubscriberBuilder):
    """PostgreSql Queued Kafka Broker Subscriber Builder class."""

    def __init__(self, *args, **kwargs):
        super().__init__(
            *args,
            impl_builder=KafkaBrokerSubscriberBuilder.new(),
            queue_builder=PostgreSqlBrokerSubscriberQueueBuilder.new(),
            **kwargs,
        )


class InMemoryQueuedKafkaBrokerSubscriberBuilder(QueuedBrokerSubscriberBuilder):
    """In Memory Queued Kafka Broker Subscriber Builder class."""

    def __init__(self, *args, **kwargs):
        super().__init__(
            *args,
            impl_builder=KafkaBrokerSubscriberBuilder.new(),
            queue_builder=InMemoryBrokerSubscriberQueueBuilder.new(),
            **kwargs,
        )


class KafkaBrokerSubscriber(BrokerSubscriber):
    """Kafka Broker Subscriber class."""

    def __init__(
        self,
        topics: Iterable[str],
        broker_host: str,
        broker_port: int,
        group_id: Optional[str] = None,
        remove_topics_on_destroy: bool = False,
        circuit_breaker_time: Union[int, float] = 3,
        **kwargs,
    ):
        super().__init__(topics, **kwargs)
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.group_id = group_id

        self.remove_topics_on_destroy = remove_topics_on_destroy

        self._circuit_breaker = CircuitBreaker(
            error_ratio=0.2, response_time=circuit_breaker_time, exceptions=[KafkaError]
        )

    @classmethod
    def _from_config(cls, config: MinosConfig, **kwargs) -> KafkaBrokerSubscriber:
        # noinspection PyTypeChecker
        return KafkaBrokerSubscriberBuilder.new().with_config(config).with_kwargs(kwargs).build()

    async def _setup(self) -> None:
        await super()._setup()
        await self._create_topics()
        await self._start_client()

    async def _destroy(self) -> None:
        await self._stop_client()
        await self._delete_topics()
        await self._stop_admin_client()
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
            await wait_for(self.client.stop(), 0.5)

    async def _stop_admin_client(self):
        await self._with_circuit_breaker(self.admin_client.close)

    async def _create_topics(self) -> None:
        logger.info(f"Creating {self.topics!r} topics...")

        new_topics = list()
        for topic in self.topics:
            new_topics.append(NewTopic(name=topic, num_partitions=1, replication_factor=1))

        def _fn() -> None:
            with suppress(TopicAlreadyExistsError):
                self.admin_client.create_topics(new_topics)

        await self._with_circuit_breaker(_fn)

    async def _delete_topics(self) -> None:
        if not self.remove_topics_on_destroy:
            return

        logger.info(f"Deleting {self.topics!r} topics...")
        fn = partial(self.admin_client.delete_topics, list(self.topics))
        await self._with_circuit_breaker(fn)

    @cached_property
    def admin_client(self):
        """Get the kafka admin client.

        :return: An ``KafkaAdminClient`` instance.
        """
        return KafkaAdminClient(bootstrap_servers=f"{self.broker_host}:{self.broker_port}")

    async def _receive(self) -> BrokerMessage:
        record = await self.client.getone()
        bytes_ = record.value
        message = BrokerMessage.from_avro_bytes(bytes_)
        return message

    async def _with_circuit_breaker(self, fn: Callable[[], Union[Awaitable[R], R]]) -> R:
        while True:
            try:
                with self._circuit_breaker.context():
                    ans = fn()
                    if isawaitable(ans):
                        ans = await ans
                    return ans
            except CircuitBroken:
                await sleep(self._circuit_breaker_timeout)
            except KafkaError as exc:
                logger.warning(f"A kafka exception was raised: {exc!r}")
                await sleep(self._exception_timeout)

    @property
    def _circuit_breaker_timeout(self) -> float:
        return self._circuit_breaker.response_time * 0.5

    @property
    def _exception_timeout(self) -> float:
        return self._circuit_breaker.response_time * 0.05

    @property
    def circuit_breaker(self) -> CircuitBreaker:
        """Get the circuit breaker.

        :return: A ``CircuitBreaker`` instance.
        """
        return self._circuit_breaker

    @cached_property
    def client(self) -> AIOKafkaConsumer:
        """Get the kafka consumer client.

        :return: An ``AIOKafkaConsumer`` instance.
        """
        return AIOKafkaConsumer(
            *self.topics,
            bootstrap_servers=f"{self.broker_host}:{self.broker_port}",
            group_id=self.group_id,
            auto_offset_reset="earliest",
        )
