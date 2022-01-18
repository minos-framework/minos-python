from __future__ import (
    annotations,
)

import logging

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

    def __init__(self, *args, broker_host: str, broker_port: int, **kwargs):
        super().__init__(*args, **kwargs)
        self.broker_host = broker_host
        self.broker_port = broker_port

    @classmethod
    def _from_config(cls, config: MinosConfig, **kwargs) -> KafkaBrokerPublisher:
        kwargs["broker_host"] = config.broker.host
        kwargs["broker_port"] = config.broker.port
        # noinspection PyProtectedMember
        return cls(**config.broker.queue._asdict(), **kwargs)

    async def _setup(self) -> None:
        await super()._setup()
        await self.client.start()

    async def _destroy(self) -> None:
        await self.client.stop()
        await super()._destroy()

    async def send(self, message: BrokerMessage) -> None:
        """TODO

        :param message: TODO
        :return: TODO
        """
        logger.info(f"Publishing {message!r} message...")
        await self.client.send_and_wait(message.topic, message.avro_bytes)

    @cached_property
    def client(self) -> AIOKafkaProducer:
        """Get the client instance.

        :return: An ``AIOKafkaProducer`` instance.
        """
        return AIOKafkaProducer(bootstrap_servers=f"{self.broker_host}:{self.broker_port}")
