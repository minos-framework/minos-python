"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from __future__ import annotations

from typing import (
    NamedTuple,
    NoReturn,
    Optional,
)

from aiokafka import AIOKafkaProducer
from aiomisc.service.periodic import PeriodicService
from minos.common import MinosConfig

from .abc import MinosBrokerSetup


class MinosQueueDispatcherService(PeriodicService):
    """Minos QueueDispatcherService class."""

    def __init__(self, config: MinosConfig = None, **kwargs):
        super().__init__(**kwargs)
        self.dispatcher = MinosQueueDispatcher.from_config(config=config)

    async def callback(self) -> None:
        """Method to be called periodically by the internal ``aiomisc`` logic.

        :return:This method does not return anything.
        """
        await self.dispatcher.dispatch()


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
            return None
        # noinspection PyProtectedMember
        return cls(*args, **config.events._asdict(), **kwargs)

    async def dispatch(self) -> NoReturn:
        """Dispatch the items in the publishing queue.

        :return: This method does not return anything.
        """
        async with self._connection() as connect:
            async with connect.cursor() as cur:
                # noinspection SqlRedundantOrderingDirection
                await cur.execute(
                    "SELECT * FROM producer_queue WHERE retry <= %d ORDER BY creation_date ASC LIMIT %d;"
                    % (self.retry, self.records),
                )
                async for row in cur:
                    # noinspection PyBroadException
                    try:
                        published = await self.publish(topic=row[1], message=row[2])
                    except Exception:
                        published = False
                    finally:
                        async with connect.cursor() as cur2:
                            if published:
                                # Delete from database If the event was sent successfully to Kafka.
                                await cur2.execute("DELETE FROM producer_queue WHERE id=%d;" % row[0])
                            else:
                                # Update queue retry column. Increase by 1.
                                await cur2.execute("UPDATE producer_queue SET retry = retry + 1 WHERE id=%d;" % row[0])

    async def publish(self, topic: str, message: bytes) -> bool:
        """ Publish a new item in in the broker (kafka).

        :param topic: The topic in which the message will be published.
        :param message: The message to be published.
        :return: A boolean flag, ``True`` when the message is properly published or ``False`` otherwise.
        """
        producer = AIOKafkaProducer(bootstrap_servers=f"{self.broker.host}:{self.broker.port}")
        # Get cluster layout and initial topic/partition leadership information
        await producer.start()
        # noinspection PyBroadException
        try:
            # Produce message
            await producer.send_and_wait(topic, message)
            flag = True
        except Exception:
            flag = False
        finally:
            # Wait for all pending messages to be delivered or expire.
            await producer.stop()

        return flag
