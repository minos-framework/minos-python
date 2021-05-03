"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import unittest

import aiopg
from minos.common import (
    Event,
)
from minos.common.testing import (
    PostgresAsyncTestCase,
)
from minos.networks.event import (
    MinosEventHandlerPeriodicService,
    MinosEventServer,
    event_handler_table_creation,
)
from tests.aggregate_classes import (
    AggregateTest,
)
from tests.utils import (
    BASE_PATH,
)


class TestPostgreSqlMinosEventHandler(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    async def test_if_queue_table_exists(self):
        await event_handler_table_creation(self.config)

        async with aiopg.connect(**self.events_queue_db) as connect:
            async with connect.cursor() as cur:
                await cur.execute(
                    "SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'event_queue';"
                )
                ret = []
                async for row in cur:
                    ret.append(row)

        assert ret == [(1,)]

    async def test_event_queue_add(self):
        await event_handler_table_creation(self.config)

        model = AggregateTest(test_id=1, test=2, id=1, version=1)
        event_instance = Event(topic="TestEventQueueAdd", model=model.classname, items=[])
        bin_data = event_instance.avro_bytes
        Event.from_avro_bytes(bin_data)

        m = MinosEventServer(conf=self.config)
        affected_rows, id = await m.event_queue_add(topic=event_instance.topic, partition=0, binary=bin_data)

        assert affected_rows == 1
        assert id > 0

    async def test_get_event_handler(self):
        await event_handler_table_creation(self.config)

        model = AggregateTest(test_id=1, test=2, id=1, version=1)
        event_instance = Event(topic="TestEventQueueAdd", model=model.classname, items=[])
        m = MinosEventHandlerPeriodicService(conf=self.config, interval=0.5)

        cls = m.get_event_handler(topic="TicketAdded")
        result = await cls(topic="TicketAdded", event=event_instance)

        assert result == "request_added"

    async def test_event_queue_checker(self):
        await event_handler_table_creation(self.config)

        model = AggregateTest(test_id=1, test=2, id=1, version=1)
        event_instance = Event(topic="TicketAdded", model=model.classname, items=[])
        bin_data = event_instance.avro_bytes
        Event.from_avro_bytes(bin_data)

        m = MinosEventServer(conf=self.config)
        affected_rows, id = await m.event_queue_add(topic=event_instance.topic, partition=0, binary=bin_data)

        assert affected_rows == 1
        assert id > 0

        async with aiopg.connect(**self.events_queue_db) as connect:
            async with connect.cursor() as cur:
                await cur.execute("SELECT COUNT(*) FROM event_queue WHERE id=%d" % (id))
                records = await cur.fetchone()

        assert records[0] == 1

        m = MinosEventHandlerPeriodicService(conf=self.config, interval=0.5)
        await m.event_queue_checker()

        async with aiopg.connect(**self.events_queue_db) as connect:
            async with connect.cursor() as cur:
                await cur.execute("SELECT COUNT(*) FROM event_queue WHERE id=%d" % (id))
                records = await cur.fetchone()

        assert records[0] == 0


if __name__ == "__main__":
    unittest.main()
