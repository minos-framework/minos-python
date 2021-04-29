"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from __future__ import (
    annotations,
)

from typing import (
    NamedTuple,
    Optional, NoReturn,
)

from aiokafka import (
    AIOKafkaProducer,
)
from aiomisc.service.periodic import (
    PeriodicService,
)
from minos.common import (
    MinosConfig,
)

from .abc import (
    MinosBrokerSetup,
)


class MinosQueueDispatcherService(PeriodicService):
    """TODO"""

    def __init__(self, config: MinosConfig = None, **kwargs):
        super().__init__(**kwargs)
        self.dispatcher = MinosQueueDispatcher.from_config(config=config)

    async def callback(self):
        """TODO

        :return:TODO
        """
        await self.dispatcher.dispatch()


class MinosQueueDispatcher(MinosBrokerSetup):
    """TODO"""

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
        """TODO

        :return: TODO
        """
        async with self._connection() as connect:
            async with connect.cursor() as cur:
                # noinspection SqlRedundantOrderingDirection
                await cur.execute(
                    "SELECT * FROM producer_queue WHERE retry <= %d ORDER BY creation_date ASC LIMIT %d;"
                    % (self.retry, self.records),
                )
                async for row in cur:
                    published = False
                    # noinspection PyBroadException
                    try:
                        published = await self.publish(topic=row[1], message=row[2])
                        if published:
                            # Delete from database If the event was sent successfully to Kafka.
                            async with connect.cursor() as cur2:
                                await cur2.execute("DELETE FROM producer_queue WHERE id=%d;" % row[0])
                    except Exception:
                        published = False
                    finally:
                        if not published:
                            # Update queue retry column. Increase by 1.
                            async with connect.cursor() as cur3:
                                await cur3.execute("UPDATE producer_queue SET retry = retry + 1 WHERE id=%d;" % row[0])

    async def publish(self, topic: str, message: bytes) -> bool:
        """ TODO

        :param topic:TODO
        :param message: TODO
        :return: TODO
        """
        producer = AIOKafkaProducer(
            bootstrap_servers="{host}:{port}".format(host=self.broker.host, port=self.broker.port)
        )
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
