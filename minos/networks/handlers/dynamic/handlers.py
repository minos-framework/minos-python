"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from __future__ import (
    annotations,
)

import logging
from asyncio import (
    TimeoutError,
    wait_for,
)
from datetime import (
    datetime,
)
from typing import (
    Any,
    NoReturn,
    Optional,
    Union,
)

from aiokafka import (
    AIOKafkaConsumer,
)
from aiopg import (
    Cursor,
)
from psycopg2.sql import (
    SQL,
    Identifier,
)

from minos.common import (
    BROKER,
    MinosConfig,
    MinosHandler,
    Model,
)

from ...exceptions import (
    MinosHandlerNotFoundEnoughEntriesException,
)
from ...utils import (
    consume_queue,
)
from ..abc import (
    Handler,
)
from ..entries import (
    HandlerEntry,
)

logger = logging.getLogger(__name__)


class DynamicHandler(MinosHandler):
    """Dynamic Handler class.`"""

    __slots__ = "_broker"

    def __init__(self, broker: Optional[BROKER] = None, **kwargs):
        super().__init__(**kwargs)
        self._broker = broker

    @property
    def broker_host(self) -> str:
        """Broker host getter.

        :return: A string value.
        """
        return self._broker.host

    @property
    def broker_port(self) -> int:
        """Broker port getter.

        :return: An integer value.
        """
        return self._broker.port

    @classmethod
    def _from_config(cls, *args, config: MinosConfig, **kwargs) -> DynamicHandler:
        return cls(broker=config.broker, **kwargs)

    async def get_one(self, *args, **kwargs) -> HandlerEntry:
        """Get one handler entry from the given topics.

        :param args: Additional positional parameters to be passed to get_many.
        :param kwargs: Additional named parameters to be passed to get_many.
        :return: A ``HandlerEntry`` instance.
        """
        return (await self.get_many(*args, **(kwargs | {"count": 1})))[0]

    async def get_many(
        self, topics: Union[str, list[str]], count: int, timeout: float = 60, **kwargs,
    ) -> list[HandlerEntry]:
        """Get multiple handler entries from the given topics.

        :param topics: The list of topics to be watched.
        :param timeout: Maximum time in seconds to wait for messages.
        :param count: Number of entries to be collected.
        :return: A list of ``HandlerEntry`` instances.
        """

        try:
            raw = await wait_for(self._get_many(topics, count), timeout=timeout)
        except TimeoutError:
            raise MinosHandlerNotFoundEnoughEntriesException(
                f"Timeout exceeded while trying to fetch {count!r} entries from {topics!r}."
            )

        def _fn(message: Any) -> HandlerEntry:
            message = self._build_tuple(message)
            return HandlerEntry(*message)

        entries = [_fn(message) for message in raw]

        logger.info(f"Obtained {entries!r} entries...")

        return entries

    async def _get_many(self, topics: Union[str, list[str]], count: int) -> list[Any]:
        if isinstance(topics, str):
            topics = [topics]

        consumer = AIOKafkaConsumer(*topics, bootstrap_servers=f"{self.broker_host}:{self.broker_port}")

        raw = list()
        try:
            await consumer.start()
            while len(raw) < count:
                raw.append(await consumer.getone())
        finally:
            await consumer.stop()

        return raw

    @staticmethod
    def _build_tuple(record: Any) -> tuple[int, str, int, bytes, int, datetime]:
        return 0, record.topic, record.partition, record.value, 0, datetime.now()


class DynamicReplyHandler(Handler):
    """Dynamic Reply Handler class."""

    TABLE_NAME = "dynamic_queue"
    ENTRY_MODEL_CLS = Model

    async def dispatch_one(self, entry: HandlerEntry) -> NoReturn:
        pass

    def __init__(self, topic, **kwargs):
        super().__init__(**kwargs)

        self.topic = topic
        self._real_topic = topic if topic.endswith("Reply") else f"{topic}Reply"

    @classmethod
    def _from_config(cls, *args, config: MinosConfig, **kwargs) -> DynamicReplyHandler:
        return cls(handlers=dict(), **config.broker.queue._asdict(), **kwargs)

    async def get_one(self, *args, **kwargs) -> HandlerEntry:
        """Get one handler entry from the given topics.

        :param args: Additional positional parameters to be passed to get_many.
        :param kwargs: Additional named parameters to be passed to get_many.
        :return: A ``HandlerEntry`` instance.
        """
        return (await self.get_many(*args, **(kwargs | {"count": 1})))[0]

    async def get_many(self, count: int, timeout: float = 60, **kwargs) -> list[HandlerEntry]:
        """Get multiple handler entries from the given topics.

        :param timeout: Maximum time in seconds to wait for messages.
        :param count: Number of entries to be collected.
        :return: A list of ``HandlerEntry`` instances.
        """
        try:
            entries = await wait_for(self._get_many(count, **kwargs), timeout=timeout)
        except TimeoutError:
            raise MinosHandlerNotFoundEnoughEntriesException(
                f"Timeout exceeded while trying to fetch {count!r} entries from {self._real_topic!r}."
            )

        logger.info(f"Dispatching '{entries if count > 1 else entries[0]!s}'...")

        return entries

    async def _get_many(self, count: int, max_wait: Optional[float] = 1.0) -> list[HandlerEntry]:
        async with self.cursor() as cursor:
            result = await self._get(cursor, count)

            if len(result) < count:
                # noinspection PyTypeChecker
                await cursor.execute(SQL("LISTEN {}").format(Identifier(self._real_topic)))
                try:
                    while len(result) < count:
                        try:
                            await wait_for(consume_queue(cursor.connection.notifies, count - len(result)), max_wait)
                        finally:
                            result += self._get(cursor, count - len(result))
                finally:
                    # noinspection PyTypeChecker
                    await cursor.execute(SQL("UNLISTEN {}").format(Identifier(self._real_topic)))

            return result

    async def _get(self, cursor: Cursor, count) -> list[HandlerEntry]:
        entries = list()
        async with cursor.begin():
            # noinspection PyTypeChecker
            await cursor.execute(_SELECT_NON_PROCESSED_ROWS_QUERY, (self._real_topic, count))
            for entry in self._build_entries(await cursor.fetchall()):
                await cursor.execute(self._queries["delete_processed"], (entry.id,))
                entries.append(entry)
        return entries


_SELECT_NON_PROCESSED_ROWS_QUERY = SQL(
    "SELECT * FROM dynamic_queue WHERE topic = %s ORDER BY creation_date LIMIT %s FOR UPDATE SKIP LOCKED"
)
