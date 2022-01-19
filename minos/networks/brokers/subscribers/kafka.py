from __future__ import (
    annotations,
)

from collections.abc import (
    AsyncIterator,
)

from aiokafka import (
    AIOKafkaConsumer,
    ConsumerRecord,
)
from cached_property import (
    cached_property,
)
from kafka.errors import (
    KafkaError,
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


class KafkaBrokerSubscriber(BrokerSubscriber):
    """TODO"""

    def __init__(self, *args, broker_host: str, broker_port: int, group_id: str, **kwargs):
        super().__init__(*args, **kwargs)
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.group_id = group_id

    @classmethod
    def _from_config(cls, config: MinosConfig, **kwargs) -> KafkaBrokerSubscriber:
        if "group_id" not in kwargs:
            kwargs["group_id"] = config.service.name
        return cls(broker_host=config.broker.host, broker_port=config.broker.port, **kwargs)

    async def _setup(self) -> None:
        await super()._setup()
        await self.client.start()

    async def _destroy(self) -> None:
        try:
            await self.client.stop()
        except KafkaError:  # pragma: no cover
            pass
        await super()._destroy()

    async def receive_all(self) -> AsyncIterator[BrokerMessage]:
        """TODO

        :return: TODO

        """
        async for record in self.client:
            yield self._dispatch_one(record)

    @staticmethod
    def _dispatch_one(record: ConsumerRecord) -> BrokerMessage:
        bytes_ = record.value
        return BrokerMessage.from_avro_bytes(bytes_)

    @cached_property
    def client(self) -> AIOKafkaConsumer:
        """Get the kafka consumer client.

        :return: An ``AIOKafkaConsumer`` instance.
        """
        return AIOKafkaConsumer(
            *self._topics,
            bootstrap_servers=f"{self.broker_host}:{self.broker_port}",
            group_id=self.group_id,
            auto_offset_reset="earliest",
        )
