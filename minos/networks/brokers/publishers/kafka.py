from __future__ import (
    annotations,
)

import logging
from typing import (
    Optional,
)

from aiokafka import (
    AIOKafkaProducer,
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

    def __init__(
        self, *args, broker_host: str, broker_port: int, client: Optional[AIOKafkaProducer] = None, **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.broker_host = broker_host
        self.broker_port = broker_port
        self._client = client

    @classmethod
    def _from_config(cls, config: MinosConfig, **kwargs) -> KafkaBrokerPublisher:
        kwargs["broker_host"] = config.broker.host
        kwargs["broker_port"] = config.broker.port
        # noinspection PyProtectedMember
        return cls(**config.broker.queue._asdict(), **kwargs)

    async def send(self, message: BrokerMessage) -> None:
        """TODO

        :param message: TODO
        :return: TODO
        """
        await self._send(message.topic, message.avro_bytes)

    async def _send(self, topic: str, message: bytes) -> bool:
        logger.debug(f"Producing message with {topic!s} topic...")

        # noinspection PyBroadException
        try:
            await self.client.send_and_wait(topic, message)
            return True
        except Exception:
            return False

    @property
    def client(self) -> AIOKafkaProducer:
        """Get the client instance.

        :return: An ``AIOKafkaProducer`` instance.
        """
        if self._client is None:  # pragma: no cover
            self._client = AIOKafkaProducer(bootstrap_servers=f"{self.broker_host}:{self.broker_port}")
        return self._client
