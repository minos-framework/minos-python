"""minos.networks.brokers.producers module."""

from __future__ import (
    annotations,
)

import logging
from typing import (
    NoReturn,
)

from aiokafka import (
    AIOKafkaProducer,
)
from psycopg2.sql import (
    SQL,
)

from minos.common import (
    MinosConfig,
)

from ..utils import (
    consume_queue,
)
from .abc import (
    BrokerSetup,
)

logger = logging.getLogger(__name__)


class Producer(BrokerSetup):
    """Minos Queue Dispatcher Class."""

    # noinspection PyUnresolvedReferences
    def __init__(self, *args, broker_host: str, broker_port: int, retry: int, records: int, **kwargs):
        # noinspection PyProtectedMember
        super().__init__(*args, **kwargs)
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.retry = retry
        self.records = records

    @classmethod
    def _from_config(cls, *args, config: MinosConfig, **kwargs) -> Producer:
        return cls(
            *args,
            broker_host=config.broker.host,
            broker_port=config.broker.port,
            **config.broker.queue._asdict(),
            **kwargs,
        )

    async def dispatch(self) -> NoReturn:
        """Dispatch the items in the publishing queue.

        :return: This method does not return anything.
        """
        async with self.cursor() as cursor:
            await cursor.execute("LISTEN producer_queue;")
            try:
                while True:
                    await consume_queue(cursor.connection.notifies, self.records)

                    async with cursor.begin():
                        # noinspection PyTypeChecker
                        await cursor.execute(_SELECT_NON_PROCESSED_ROWS_QUERY, (self.retry, self.records))
                        result = await cursor.fetchall()

                        for row in result:
                            published = False
                            try:
                                published = await self.publish(topic=row[1], message=row[2])
                            except Exception as exc:  # pragma: no cover
                                logger.warning(f"Raised an exception while publishing a message: {exc!r}")
                            finally:
                                if published:
                                    # noinspection PyTypeChecker
                                    await cursor.execute(_DELETE_PROCESSED_QUERY, (row[0],))
                                else:
                                    # noinspection PyTypeChecker
                                    await cursor.execute(_UPDATE_NON_PROCESSED_QUERY, (row[0],))
            finally:
                await cursor.execute("UNLISTEN producer_queue;")

    async def publish(self, topic: str, message: bytes) -> bool:
        """Publish a new item in the broker (kafka).

        :param topic: The topic in which the message will be published.
        :param message: The message to be published.
        :return: A boolean flag, ``True`` when the message is properly published or ``False`` otherwise.
        """
        logger.debug(f"Producing message with {topic!s} topic...")

        producer = AIOKafkaProducer(bootstrap_servers=f"{self.broker_host}:{self.broker_port}")
        # noinspection PyBroadException
        try:
            # Get cluster layout and initial topic/partition leadership information
            await producer.start()
            # Produce message
            await producer.send_and_wait(topic, message)
            published = True
        except Exception:
            published = False
        finally:
            # Wait for all pending messages to be delivered or expire.
            await producer.stop()

        return published


_SELECT_NON_PROCESSED_ROWS_QUERY = SQL(
    "SELECT * "
    "FROM producer_queue "
    "WHERE retry < %s "
    "ORDER BY creation_date "
    "LIMIT %s "
    "FOR UPDATE "
    "SKIP LOCKED"
)

_DELETE_PROCESSED_QUERY = SQL("DELETE FROM producer_queue " "WHERE id = %s")

_UPDATE_NON_PROCESSED_QUERY = SQL("UPDATE producer_queue " "SET retry = retry + 1 " "WHERE id = %s")
