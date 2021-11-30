from __future__ import (
    annotations,
)

import logging
from typing import (
    Any,
    Optional,
)
from uuid import (
    UUID,
)

from psycopg2.sql import (
    SQL,
)

from minos.common import (
    MinosConfig,
)

from ..messages import (
    BrokerMessage,
    BrokerMessageStatus,
    BrokerMessageStrategy,
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
    async def send(
        self,
        data: Any,
        topic: str,
        *,
        identifier: Optional[UUID] = None,
        reply_topic: Optional[str] = None,
        user: Optional[UUID] = None,
        status: BrokerMessageStatus = BrokerMessageStatus.SUCCESS,
        strategy: BrokerMessageStrategy = BrokerMessageStrategy.UNICAST,
        headers: Optional[dict[str, str]] = None,
        **kwargs,
    ) -> UUID:
        """Send a ``BrokerMessage``.

        :param data: The data to be send.
        :param topic: Topic in which the message will be published.
        :param identifier: The identifier of the message.
        :param reply_topic: An optional topic name to wait for a response.
        :param user: The user identifier that send the message.
        :param status: The status code of the message.
        :param strategy: The publishing strategy.
        :param headers: A mapping of string values identified by a string key.
        :param kwargs: Additional named arguments.
        :return: The ``UUID`` identifier of the message.
        """

        message = BrokerMessage(
            topic=topic,
            data=data,
            identifier=identifier,
            status=status,
            reply_topic=reply_topic,
            user=user,
            strategy=strategy,
            headers=headers,
        )
        logger.info(f"Publishing '{message!s}'...")
        await self.enqueue(message.topic, message.strategy, message.avro_bytes)
        return message.identifier

    async def enqueue(self, topic: str, strategy: BrokerMessageStrategy, raw: bytes) -> int:
        """Send a sequence of bytes to the given topic.

        :param topic: Topic in which the bytes will be send.
        :param strategy: The publishing strategy.
        :param raw: Bytes sequence to be send.
        :return: The identifier of the message in the queue.
        """
        params = (topic, raw, strategy)
        raw = await self.submit_query_and_fetchone(_INSERT_ENTRY_QUERY, params)
        await self.submit_query(_NOTIFY_QUERY)
        return raw[0]


_INSERT_ENTRY_QUERY = SQL("INSERT INTO producer_queue (topic, data, strategy) VALUES (%s, %s, %s) RETURNING id")

_NOTIFY_QUERY = SQL("NOTIFY producer_queue")
