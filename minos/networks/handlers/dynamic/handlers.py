"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from __future__ import (
    annotations,
)

from datetime import (
    datetime,
)
from itertools import (
    chain,
)
from typing import (
    Optional,
)

from aiokafka import (
    AIOKafkaConsumer,
)

from minos.common import (
    BROKER,
    MinosConfig,
    MinosSetup,
    Model,
)

from ..entries import (
    HandlerEntry,
)


class DynamicHandler(MinosSetup):
    """TODO"""

    __slots__ = ("_broker",)

    def __init__(self, broker: Optional[BROKER] = None, **kwargs):
        super().__init__(**kwargs)
        self._broker = broker

    @classmethod
    def _from_config(cls, *args, config: MinosConfig, **kwargs) -> DynamicHandler:
        return cls(broker=config.saga.broker, **kwargs)

    async def get_one(self, topics: list[str], timeout: float = 0) -> HandlerEntry:
        """TODO

        :param topics: TODO
        :param timeout: TODO
        :return: TODO
        """
        entries = await self.get_many(topics, timeout=timeout, max_records=1)
        if not len(entries):
            raise ValueError()  # TODO: raise meaningful exception.
        return entries[0]

    async def get_many(
        self, topics: list[str], timeout: float = 0, max_records: Optional[int] = None
    ) -> list[HandlerEntry]:
        """TODO

        :param topics: TODO
        :param timeout: TODO
        :param max_records: TODO
        :return: TODO
        """
        result = await self._get_many(topics, timeout, max_records)
        return [await self._build_entry(self._build_tuple(message)) for message in chain(*result.values())]

    async def _get_many(
        self, topics: list[str], timeout: float = 0, max_records: Optional[int] = None
    ) -> dict[str, tuple]:
        consumer = AIOKafkaConsumer(*topics, bootstrap_servers=f"{self._broker.host}:{self._broker.port}")

        try:
            await consumer.start()
            return await consumer.getmany(timeout_ms=int(timeout * 1000), max_records=max_records)
        finally:
            await consumer.stop()

    @staticmethod
    def _build_tuple(record):
        return 0, record.topic, record.partition, record.value, 0, datetime.now()

    @staticmethod
    async def _build_entry(row: tuple[int, str, int, bytes, datetime]) -> HandlerEntry:
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
