from __future__ import (
    annotations,
)

import logging

from psycopg2.sql import (
    SQL,
)

from minos.common import (
    MinosConfig,
)

from ..messages import (
    BrokerMessage,
    BrokerMessageV1Strategy,
)
from .abc import (
    BrokerPublisherSetup,
)

logger = logging.getLogger(__name__)


class BrokerPublisher(BrokerPublisherSetup):
    """Broker Publisher class."""

    @classmethod
    def _from_config(cls, *args, config: MinosConfig, **kwargs) -> BrokerPublisher:
        # noinspection PyProtectedMember
        return cls(*args, **config.broker.queue._asdict(), **kwargs)

    # noinspection PyMethodOverriding
    async def send(self, message: BrokerMessage) -> None:
        """Send a ``BrokerMessage``.

        :param message: The message to be sent.
        :return: The ``UUID`` identifier of the message.
        """
        logger.info(f"Publishing '{message!s}'...")
        await self.enqueue(message.topic, message.strategy, message.avro_bytes)

    async def enqueue(self, topic: str, strategy: BrokerMessageV1Strategy, raw: bytes) -> int:
        """Send a sequence of bytes to the given topic.

        :param topic: Topic in which the bytes will be sent.
        :param strategy: The publishing strategy.
        :param raw: Bytes sequence to be sent.
        :return: The identifier of the message in the queue.
        """
        params = (topic, raw, strategy)
        raw = await self.submit_query_and_fetchone(_INSERT_ENTRY_QUERY, params)
        await self.submit_query(_NOTIFY_QUERY)
        return raw[0]


_INSERT_ENTRY_QUERY = SQL("INSERT INTO producer_queue (topic, data, strategy) VALUES (%s, %s, %s) RETURNING id")

_NOTIFY_QUERY = SQL("NOTIFY producer_queue")
