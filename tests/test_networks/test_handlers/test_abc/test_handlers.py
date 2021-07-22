"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import unittest
from asyncio import gather
from collections import namedtuple
from typing import NoReturn
from uuid import uuid4

import aiopg

from minos.common import DataTransferObject
from minos.common.testing import PostgresAsyncTestCase
from minos.networks import (
    Handler,
    MinosActionNotFoundException,
    EnrouteDecoratorAnalyzer,
)
from minos.networks.handlers import HandlerEntry
from tests.services.CommandTestService import CommandService
from tests.utils import (
    BASE_PATH,
    FAKE_AGGREGATE_DIFF,
)
from importlib import import_module


class _FakeHandler(Handler):
    TABLE_NAME = "fake"
    ENTRY_MODEL_CLS = DataTransferObject

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.call_count = 0
        self.call_args = None

    async def dispatch_one(self, entry: HandlerEntry) -> NoReturn:
        """For testing purposes."""
        self.call_count += 1
        self.call_args = (entry,)
        if entry.topic == "DeleteOrder":
            raise ValueError()


class TestHandler(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def handlers(self):
        p, m = self.config.commands.service.rsplit(".", 1)
        mod = import_module(p)
        met = getattr(mod, m)

        decorators = EnrouteDecoratorAnalyzer(met).command()

        handlers = {}
        for key, value in decorators.items():
            for v in decorators[key]:
                for topic in v.topics:
                    handlers[topic] = key

        return handlers

    def setUp(self) -> None:
        super().setUp()
        handlers = self.handlers()
        handlers["empty"] = None
        self.handler = _FakeHandler(handlers=handlers, **self.config.commands.queue._asdict())

    async def test_get_action(self):
        action = self.handler.get_action(topic="AddOrder")
        self.assertEqual("wrapper", action.__name__)

    async def test_get_action_none(self):
        action = self.handler.get_action(topic="empty")
        self.assertIsNone(None, action)

    async def test_get_action_raises(self):
        with self.assertRaises(MinosActionNotFoundException) as context:
            self.handler.get_action(topic="NotExisting")

        self.assertTrue(
            "topic NotExisting have no controller/action configured, please review th configuration file"
            in str(context.exception)
        )

    async def test_dispatch(self):
        from minos.common import Event

        instance = Event("AddOrder", FAKE_AGGREGATE_DIFF)

        async with self.handler:
            queue_id = await self._insert_one(instance)
            await self.handler.dispatch()
            self.assertTrue(await self._is_processed(queue_id))

        self.assertEqual(1, self.handler.call_count)

    async def test_dispatch_wrong(self):
        from minos.common import Event

        instance_1 = namedtuple("FakeCommand", ("topic", "avro_bytes"))("AddOrder", bytes(b"Test"))
        instance_2 = Event("DeleteOrder", FAKE_AGGREGATE_DIFF)

        async with self.handler:
            queue_id_1 = await self._insert_one(instance_1)
            queue_id_2 = await self._insert_one(instance_2)
            await self.handler.dispatch()
            self.assertFalse(await self._is_processed(queue_id_1))
            self.assertFalse(await self._is_processed(queue_id_2))

    async def test_dispatch_concurrent(self):
        from minos.common import Command
        from tests.utils import FakeModel

        saga = uuid4()

        instance = Command("AddOrder", [FakeModel("foo")], saga, "UpdateTicket")
        instance_wrong = namedtuple("FakeCommand", ("topic", "avro_bytes"))("AddOrder", bytes(b"Test"))

        async with self.handler:
            for _ in range(0, 25):
                await self._insert_one(instance)
                await self._insert_one(instance_wrong)

            self.assertEqual(50, await self._count())

            await gather(*[self.handler.dispatch() for _ in range(0, 6)])

            self.assertEqual(25, await self._count())

    async def _insert_one(self, instance):
        async with aiopg.connect(**self.commands_queue_db) as connect:
            async with connect.cursor() as cur:
                await cur.execute(
                    "INSERT INTO fake (topic, partition_id, binary_data, creation_date) "
                    "VALUES (%s, %s, %s, NOW()) "
                    "RETURNING id;",
                    (instance.topic, 0, instance.avro_bytes),
                )
                return (await cur.fetchone())[0]

    async def _count(self):
        async with aiopg.connect(**self.commands_queue_db) as connect:
            async with connect.cursor() as cur:
                await cur.execute("SELECT COUNT(*) FROM fake")
                return (await cur.fetchone())[0]

    async def _is_processed(self, queue_id):
        async with aiopg.connect(**self.commands_queue_db) as connect:
            async with connect.cursor() as cur:
                await cur.execute("SELECT COUNT(*) FROM fake WHERE id=%d" % (queue_id,))
                return (await cur.fetchone())[0] == 0


if __name__ == "__main__":
    unittest.main()
