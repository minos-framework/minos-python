from __future__ import (
    annotations,
)

import logging
from asyncio import (
    TimeoutError,
    wait_for,
)
from collections.abc import (
    Iterable,
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
    AIOKafkaConsumer,
    ConsumerStoppedError,
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
    TopicAlreadyExistsError,
)

from minos.networks import (
    BrokerMessage,
    BrokerSubscriber,
    BrokerSubscriberBuilder,
)

from .common import (
    KafkaBrokerBuilderMixin,
    KafkaCircuitBreakerMixin,
)

logger = logging.getLogger(__name__)


class KafkaBrokerSubscriber(BrokerSubscriber, KafkaCircuitBreakerMixin):
    """Kafka Broker Subscriber class."""

    def __init__(
        self,
        topics: Iterable[str],
        host: Optional[str] = None,
        port: Optional[int] = None,
        group_id: Optional[str] = None,
        remove_topics_on_destroy: bool = False,
        **kwargs,
    ):
        super().__init__(topics, **kwargs)
        if host is None:
            host = "localhost"

        if port is None:
            port = 9092

        self._host = host
        self._port = port
        self._group_id = group_id

        self._remove_topics_on_destroy = remove_topics_on_destroy

    @property
    def host(self) -> str:
        """The host of kafka.

        :return: A ``str`` value.
        """
        return self._host

    @property
    def port(self) -> int:
        """The port of kafka.

        :return: A ``int`` value.
        """
        return self._port

    @property
    def group_id(self) -> Optional[str]:
        """The id of kafka's group.

        :return: An ``Optional[str]``` value.
        """
        return self._group_id

    @property
    def remove_topics_on_destroy(self) -> int:
        """Flag to check if topics should be removed on destroy.

        :return: A ``bool`` value.
        """
        return self._remove_topics_on_destroy

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
            await self.with_circuit_breaker(self.client.start)
        except Exception as exc:
            await self._stop_client()
            raise exc

    async def _stop_client(self):
        with suppress(TimeoutError):
            await wait_for(self.client.stop(), 0.5)

    async def _stop_admin_client(self):
        await self.with_circuit_breaker(self.admin_client.close)

    async def _create_topics(self) -> None:
        logger.info(f"Creating {self.topics!r} topics...")

        new_topics = list()
        for topic in self.topics:
            new_topics.append(NewTopic(name=topic, num_partitions=1, replication_factor=1))

        def _fn() -> None:
            with suppress(TopicAlreadyExistsError):
                self.admin_client.create_topics(new_topics)

        await self.with_circuit_breaker(_fn)

    async def _delete_topics(self) -> None:
        if not self.remove_topics_on_destroy:
            return

        logger.info(f"Deleting {self.topics!r} topics...")
        fn = partial(self.admin_client.delete_topics, list(self.topics))
        await self.with_circuit_breaker(fn)

    @cached_property
    def admin_client(self):
        """Get the kafka admin client.

        :return: An ``KafkaAdminClient`` instance.
        """
        return KafkaAdminClient(bootstrap_servers=f"{self.host}:{self.port}")

    async def _receive(self) -> BrokerMessage:
        try:
            record = await self.client.getone()
        except ConsumerStoppedError:
            raise StopAsyncIteration

        bytes_ = record.value
        message = BrokerMessage.from_avro_bytes(bytes_)
        return message

    @cached_property
    def client(self) -> AIOKafkaConsumer:
        """Get the kafka consumer client.

        :return: An ``AIOKafkaConsumer`` instance.
        """
        return AIOKafkaConsumer(
            *self.topics,
            bootstrap_servers=f"{self.host}:{self.port}",
            group_id=self.group_id,
            auto_offset_reset="earliest",
        )


class KafkaBrokerSubscriberBuilder(BrokerSubscriberBuilder[KafkaBrokerSubscriber], KafkaBrokerBuilderMixin):
    """Kafka Broker Subscriber Builder class."""


KafkaBrokerSubscriber.set_builder(KafkaBrokerSubscriberBuilder)
