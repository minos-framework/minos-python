from __future__ import (
    annotations,
)

import logging
from asyncio import (
    TimeoutError,
    gather,
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
from dependency_injector.wiring import (
    Provide,
    inject,
)
from psycopg2.sql import (
    SQL,
)

from minos.common import (
    MinosConfig,
    NotProvidedException,
)

from ...utils import (
    consume_queue,
)
from ..handlers import (
    BrokerConsumer,
)
from ..messages import (
    BrokerMessageStrategy,
)
from .abc import (
    BrokerPublisherSetup,
)

logger = logging.getLogger(__name__)


class BrokerProducer(BrokerPublisherSetup):
    """Broker Producer class."""

    def __init__(
        self,
        *args,
        broker_host: str,
        broker_port: int,
        retry: int,
        records: int,
        client: Optional[AIOKafkaProducer] = None,
        consumer: BrokerConsumer = Provide["broker_consumer"],
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.retry = retry
        self.records = records
        self._client = client
        self.consumer = consumer

    @classmethod
    def _from_config(cls, config: MinosConfig, **kwargs) -> BrokerProducer:
        kwargs["broker_host"] = config.broker.host
        kwargs["broker_port"] = config.broker.port
        kwargs["consumer"] = cls._get_consumer(**kwargs)
        # noinspection PyProtectedMember
        return cls(**config.broker.queue._asdict(), **kwargs)

    # noinspection PyUnusedLocal
    @staticmethod
    @inject
    def _get_consumer(
        consumer: Optional[BrokerConsumer] = None,
        broker_consumer: BrokerConsumer = Provide["broker_consumer"],
        **kwargs,
    ) -> BrokerConsumer:
        if consumer is None:
            consumer = broker_consumer
        if consumer is None or isinstance(consumer, Provide):
            raise NotProvidedException(f"A {BrokerConsumer!r} object must be provided.")
        return consumer

    async def _setup(self) -> None:
        await super()._setup()
        await self.client.start()

    async def _destroy(self) -> None:
        await self.client.stop()
        await super()._destroy()

    async def dispatch_forever(self, max_wait: Optional[float] = 60.0) -> NoReturn:
        """Dispatch the items in the publishing queue forever.

        :param max_wait: Maximum seconds to wait for notifications. If ``None`` the wait is performed until infinity.
        :return: This method does not return anything.
        """
        async with self.cursor() as cursor:
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
        count = (await cursor.fetchone())[0]
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

            rows = await cursor.fetchall()
            futures = (self.dispatch_one(row) for row in rows)
            result = zip(await gather(*futures), rows)

            for (published, row) in result:
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

    async def dispatch_one(self, row: tuple) -> bool:
        """Dispatch one row.

        :param row: A row containing the message information.
        :return: ``True`` if everything was fine or ``False`` otherwise.
        """
        topic, message, strategy = row[1], row[2], row[3]

        # noinspection PyBroadException
        try:
            if strategy == BrokerMessageStrategy.UNICAST and topic in self.consumer.topics:
                await self.consumer.enqueue(topic, -1, message)
                return True

        except Exception as exc:
            logger.warning(f"There was a problem while trying to use the consumer: {exc!r}")

        return await self.publish(topic, message)

    async def publish(self, topic: str, message: bytes) -> bool:
        """Publish a new item in the broker (kafka).

        :param topic: The topic in which the message will be published.
        :param message: The message to be published.
        :return: A boolean flag, ``True`` when the message is properly published or ``False`` otherwise.
        """
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


# noinspection SqlDerivedTableAlias
_COUNT_NOT_PROCESSED_QUERY = SQL(
    "SELECT COUNT(*) FROM (SELECT id FROM producer_queue WHERE retry < %s FOR UPDATE SKIP LOCKED) s"
)

_SELECT_NOT_PROCESSED_QUERY = SQL(
    "SELECT * "
    "FROM producer_queue "
    "WHERE retry < %s "
    "ORDER BY created_at "
    "LIMIT %s "
    "FOR UPDATE "
    "SKIP LOCKED"
)

_DELETE_PROCESSED_QUERY = SQL("DELETE FROM producer_queue WHERE id = %s")

_UPDATE_NOT_PROCESSED_QUERY = SQL("UPDATE producer_queue SET retry = retry + 1, updated_at = NOW() WHERE id = %s")

_LISTEN_QUERY = SQL("LISTEN producer_queue")

_UNLISTEN_QUERY = SQL("UNLISTEN producer_queue")
