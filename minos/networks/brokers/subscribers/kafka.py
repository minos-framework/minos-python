from __future__ import (
    annotations,
)

import logging
from contextlib import (
    suppress,
)
from typing import (
    Iterable,
    Optional,
)

from aiokafka import (
    AIOKafkaConsumer,
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

from minos.common import (
    MinosConfig,
)

from ..messages import (
    BrokerMessage,
)
from .abc import (
    BrokerSubscriber,
)

logger = logging.getLogger(__name__)


class KafkaBrokerSubscriber(BrokerSubscriber):
    """Kafka Broker Subscriber class."""

    def __init__(
        self,
        topics: Iterable[str],
        broker_host: str,
        broker_port: int,
        group_id: Optional[str] = None,
        remove_topics_on_destroy: bool = False,
        **kwargs,
    ):
        super().__init__(topics, **kwargs)
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.group_id = group_id

        self.remove_topics_on_destroy = remove_topics_on_destroy

    @classmethod
    def _from_config(cls, config: MinosConfig, **kwargs) -> KafkaBrokerSubscriber:
        if "group_id" not in kwargs:
            kwargs["group_id"] = config.service.name
        return cls(broker_host=config.broker.host, broker_port=config.broker.port, **kwargs)

    async def _setup(self) -> None:
        await super()._setup()
        self._create_topics()
        await self.client.start()

    async def _destroy(self) -> None:
        await self.client.stop()
        self._delete_topics()
        self.admin_client.close()
        await super()._destroy()

    def _create_topics(self) -> None:
        logger.info(f"Creating {self.topics!r} topics...")

        new_topics = list()
        for topic in self.topics:
            new_topics.append(NewTopic(name=topic, num_partitions=1, replication_factor=1))

        with suppress(TopicAlreadyExistsError):
            self.admin_client.create_topics(new_topics)

    def _delete_topics(self) -> None:
        if not self.remove_topics_on_destroy:
            return

        logger.info(f"Deleting {self.topics!r} topics...")
        self.admin_client.delete_topics(list(self.topics))

    @cached_property
    def admin_client(self):
        """Get the kafka admin client.

        :return: An ``KafkaAdminClient`` instance.
        """
        return KafkaAdminClient(bootstrap_servers=f"{self.broker_host}:{self.broker_port}")

    async def receive(self) -> BrokerMessage:
        """Receive a new message.

         :return: A ``BrokerMessage`` instance.
         """
        record = await self.client.getone()
        bytes_ = record.value
        message = BrokerMessage.from_avro_bytes(bytes_)
        logger.info(f"Consuming {message!r} message...")
        return message

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
