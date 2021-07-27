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

from minos.common import (
    BROKER,
    MinosConfig,
    MinosHandler,
    Model,
)

from ...exceptions import (
    MinosHandlerNotFoundEnoughEntriesException,
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

        async def _fn(message: Any) -> HandlerEntry:
            message = self._build_tuple(message)
            return await self._build_entry(message)

        try:
            raw = await wait_for(self._get_many(topics, count), timeout=timeout)
        except TimeoutError:
            raise MinosHandlerNotFoundEnoughEntriesException(
                f"Timeout exceeded while trying to fetch {count!r} entries from {topics!r}."
            )

        entries = [await _fn(message) for message in raw]

        logger.info(f"Obtained {[v.data for v in entries]} entries...")

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

    @staticmethod
    async def _build_entry(row: tuple[int, str, int, bytes, int, datetime]) -> HandlerEntry:
        # TODO: Refactor this method.

        id = row[0]
        topic = row[1]
        callback = None
        partition_id = row[2]
        data = Model.from_avro_bytes(row[3])
        retry = row[4]
        created_at = row[5]

        entry = HandlerEntry(id, topic, callback, partition_id, data, retry, created_at)
        return entry


class DynamicReplyHandler(MinosHandler):
    """Dynamic Reply Handler class."""

    def __init__(self, topic, broker: Optional[BROKER] = None, **kwargs):
        super().__init__(**kwargs)
        self._broker = broker

        self.topic = topic

        self._real_topic = topic if topic.endswith("Reply") else f"{topic}Reply"
        self.consumer = AIOKafkaConsumer(self._real_topic, bootstrap_servers=f"{self.broker_host}:{self.broker_port}")

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
    def _from_config(cls, *args, config: MinosConfig, **kwargs) -> DynamicReplyHandler:
        return cls(broker=config.broker, **kwargs)

    async def _setup(self) -> NoReturn:
        await self.consumer.start()

    async def _destroy(self) -> NoReturn:
        await self.consumer.stop()

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

        async def _fn(message: Any) -> HandlerEntry:
            message = self._build_tuple(message)
            return await self._build_entry(message)

        try:
            raw = await wait_for(self._get_many(count), timeout=timeout)
        except TimeoutError:
            raise MinosHandlerNotFoundEnoughEntriesException(
                f"Timeout exceeded while trying to fetch {count!r} entries from {self._real_topic!r}."
            )

        entries = [await _fn(message) for message in raw]

        logger.info(f"Obtained {[v.data for v in entries]} entries...")

        return entries

    async def _get_many(self, count: int) -> list[Any]:

        raw = list()
        while len(raw) < count:
            raw.append(await self.consumer.getone())

        return raw

    @staticmethod
    def _build_tuple(record: Any) -> tuple[int, str, int, bytes, int, datetime]:
        return 0, record.topic, record.partition, record.value, 0, datetime.now()

    @staticmethod
    async def _build_entry(row: tuple[int, str, int, bytes, int, datetime]) -> HandlerEntry:
        # TODO: Refactor this method.

        id = row[0]
        topic = row[1]
        callback = None
        partition_id = row[2]
        data = Model.from_avro_bytes(row[3])
        retry = row[4]
        created_at = row[5]

        entry = HandlerEntry(id, topic, callback, partition_id, data, retry, created_at)
        return entry
