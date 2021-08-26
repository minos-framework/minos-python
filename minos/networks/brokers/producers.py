"""minos.networks.brokers.producers module."""

from __future__ import (
    annotations,
)

import logging
from asyncio import (
    TimeoutError,
    wait_for,
)
from typing import (
    NoReturn,
    Optional,
)

from aiokafka import (
    AIOKafkaProducer,
)
from aiopg import (
    Cursor,
)
from cached_property import (
    cached_property,
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

    def __init__(self, *args, broker_host: str, broker_port: int, retry: int, records: int, **kwargs):
        super().__init__(*args, **kwargs)
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.retry = retry
        self.records = records

    @classmethod
    def _from_config(cls, *args, config: MinosConfig, **kwargs) -> Producer:
        # noinspection PyProtectedMember
        return cls(
            *args,
            broker_host=config.broker.host,
            broker_port=config.broker.port,
            **config.broker.queue._asdict(),
            **kwargs,
        )

    async def dispatch_forever(self, max_wait: Optional[float] = 15.0) -> NoReturn:
        """Dispatch the items in the publishing queue forever.

        :param max_wait: Maximum seconds to wait for notifications. If ``None`` the wait is performed until infinity.
        :return: This method does not return anything.
        """
        async with self.cursor() as cursor:
            await self.dispatch(cursor)

            await cursor.execute(self._queries["listen"])
            try:
                while True:
                    await self._wait_for_entries(cursor, max_wait)
                    await self.dispatch(cursor)

            finally:
                await cursor.execute(self._queries["unlisten"])

    async def _wait_for_entries(self, cursor: Cursor, max_wait: Optional[float]) -> None:
        if await self._get_count(cursor):
            return

        while True:
            try:
                return await wait_for(consume_queue(cursor.connection.notifies, self.records), max_wait)
            except TimeoutError:
                if await self._get_count(cursor):
                    return

    async def _get_count(self, cursor) -> int:
        await cursor.execute(self._queries["count_not_processed"], (self.retry,))
        count = await cursor.fetchone()
        return count

    async def dispatch(self, cursor: Optional[Cursor] = None) -> None:
        """Dispatch the items in the publishing queue.

        :return: This method does not return anything.
        """
        is_external_cursor = cursor is not None
        if not is_external_cursor:
            cursor = await self.cursor().__aenter__()

        async with cursor.begin():
            await cursor.execute(self._queries["select_not_processed"], (self.retry, self.records))
            result = await cursor.fetchall()

            for row in result:
                published = False
                try:
                    published = await self.publish(topic=row[1], message=row[2])
                except Exception as exc:  # pragma: no cover
                    logger.warning(f"Raised an exception while publishing a message: {exc!r}")
                finally:
                    query_id = "delete_processed" if published else "update_not_processed"
                    await cursor.execute(self._queries[query_id], (row[0],))

        if not is_external_cursor:
            await cursor.__aexit__(None, None, None)

    @cached_property
    def _queries(self) -> dict[str, str]:
        # noinspection PyTypeChecker
        return {
            "listen": _LISTEN_QUERY,
            "unlisten": _UNLISTEN_QUERY,
            "count_not_processed": _COUNT_NOT_PROCESSED_QUERY,
            "select_not_processed": _SELECT_NOT_PROCESSED_QUERY,
            "delete_processed": _DELETE_PROCESSED_QUERY,
            "update_not_processed": _UPDATE_NOT_PROCESSED_QUERY,
        }

    async def publish(self, topic: str, message: bytes) -> bool:
        """Publish a new item in the broker (kafka).

        :param topic: The topic in which the message will be published.
        :param message: The message to be published.
        :return: A boolean flag, ``True`` when the message is properly published or ``False`` otherwise.
        """
        logger.debug(f"Producing message with {topic!s} topic...")

        client = AIOKafkaProducer(bootstrap_servers=f"{self.broker_host}:{self.broker_port}")
        # noinspection PyBroadException
        try:
            await client.start()
            await client.send_and_wait(topic, message)
            published = True
        except Exception:
            published = False
        finally:
            await client.stop()

        return published


# noinspection SqlDerivedTableAlias
_COUNT_NOT_PROCESSED_QUERY = SQL(
    "SELECT COUNT(*) FROM (SELECT id FROM producer_queue WHERE retry < %s FOR UPDATE SKIP LOCKED) s"
)

_SELECT_NOT_PROCESSED_QUERY = SQL(
    "SELECT * "
    "FROM producer_queue "
    "WHERE retry < %s "
    "ORDER BY creation_date "
    "LIMIT %s "
    "FOR UPDATE "
    "SKIP LOCKED"
)

_DELETE_PROCESSED_QUERY = SQL("DELETE FROM producer_queue WHERE id = %s")

_UPDATE_NOT_PROCESSED_QUERY = SQL("UPDATE producer_queue SET retry = retry + 1 WHERE id = %s")

_LISTEN_QUERY = SQL("LISTEN producer_queue")

_UNLISTEN_QUERY = SQL("UNLISTEN producer_queue")
