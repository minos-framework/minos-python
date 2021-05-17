"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from __future__ import (
    annotations,
)

from typing import (
    AsyncIterator,
    NamedTuple,
    NoReturn,
    Optional,
)

from aiokafka import (
    AIOKafkaProducer,
)

from minos.common import (
    MinosConfig,
    MinosConfigException,
)

from .abc import (
    MinosBrokerSetup,
)


class MinosQueueDispatcher(MinosBrokerSetup):
    """Minos Queue Dispatcher Class."""

    # noinspection PyUnresolvedReferences
    def __init__(self, *args, queue: NamedTuple, broker, **kwargs):
        # noinspection PyProtectedMember
        super().__init__(*args, **queue._asdict(), **kwargs)
        self.retry = queue.retry
        self.records = queue.records
        self.broker = broker

    @classmethod
    def from_config(cls, *args, config: MinosConfig = None, **kwargs) -> Optional[MinosQueueDispatcher]:
        """Build a new repository from config.
        :param args: Additional positional arguments.
        :param config: Config instance. If `None` is provided, default config is chosen.
        :param kwargs: Additional named arguments.
        :return: A `MinosRepository` instance.
        """
        if config is None:
            config = MinosConfig.get_default()
        if config is None:
            raise MinosConfigException("The config object must be setup.")
        # noinspection PyProtectedMember
        return cls(*args, **config.events._asdict(), **kwargs)

    async def dispatch(self) -> NoReturn:
        """Dispatch the items in the publishing queue.

        :return: This method does not return anything.
        """
        async for row in self.select():
            await self._dispatch_one(row)

    # noinspection PyUnusedLocal
    async def select(self, *args, **kwargs) -> AsyncIterator[tuple]:
        """Select a sequence of ``MinosSnapshotEntry`` objects.

        :param args: Additional positional arguments.
        :param kwargs: Additional named arguments.
        :return: A sequence of ``MinosSnapshotEntry`` objects.
        """
        params = (self.retry, self.records)
        async for row in self.submit_query_and_iter(_SELECT_NON_PROCESSED_ROWS_QUERY, params):
            yield row

    async def _dispatch_one(self, row: tuple) -> NoReturn:
        # noinspection PyBroadException
        published = await self.publish(topic=row[1], message=row[2])
        await self._update_queue_state(row, published)

    async def _update_queue_state(self, row: tuple, published: bool):
        update_query = _DELETE_PROCESSED_QUERY if published else _UPDATE_NON_PROCESSED_QUERY
        await self.submit_query(update_query, (row[0],))

    async def publish(self, topic: str, message: bytes) -> bool:
        """Publish a new item in in the broker (kafka).

        :param topic: The topic in which the message will be published.
        :param message: The message to be published.
        :return: A boolean flag, ``True`` when the message is properly published or ``False`` otherwise.
        """
        producer = AIOKafkaProducer(bootstrap_servers=f"{self.broker.host}:{self.broker.port}")
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


_SELECT_NON_PROCESSED_ROWS_QUERY = """
SELECT *
FROM producer_queue
WHERE retry <= %s
ORDER BY creation_date
LIMIT %s;
""".strip()

_DELETE_PROCESSED_QUERY = """
DELETE FROM producer_queue
WHERE id = %s;
""".strip()

_UPDATE_NON_PROCESSED_QUERY = """
UPDATE producer_queue
    SET retry = retry + 1
WHERE id = %s;
""".strip()
