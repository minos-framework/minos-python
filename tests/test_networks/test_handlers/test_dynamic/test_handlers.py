"""tests.test_networks.test_handlers.test_dynamic.test_handlers module."""

import unittest
from asyncio import (
    gather,
    sleep,
)
from datetime import (
    timedelta,
)

import aiopg

from minos.common.testing import (
    PostgresAsyncTestCase,
)
from minos.networks import (
    DynamicReplyHandler,
    HandlerEntry,
    HandlerSetup,
    MinosHandlerNotFoundEnoughEntriesException,
)
from tests.utils import (
    BASE_PATH,
    FakeModel,
    Message,
)


class TestDynamicReplyHandler(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def setUp(self) -> None:
        super().setUp()
        self.topic = "foo"

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.handler = DynamicReplyHandler.from_config(config=self.config, topic=self.topic)

    async def test_setup_destroy(self):
        self.assertFalse(self.handler.already_setup)
        async with self.handler:
            self.assertTrue(self.handler.already_setup)
        self.assertTrue(self.handler.already_destroyed)

    def test_base_classes(self):
        self.assertIsInstance(self.handler, HandlerSetup)

    async def test_get_one(self):
        expected = HandlerEntry(1, "fooReply", 0, FakeModel("test1").avro_bytes)
        async with self.handler:
            await self._insert_one(Message("foo", 0, FakeModel("test1").avro_bytes))
            await self._insert_one(Message("foo", 0, FakeModel("test2").avro_bytes))

            observed = await self.handler.get_one()

        self._assert_equal_entries(expected, observed)

    async def test_get_many(self):
        expected = [
            HandlerEntry(1, "fooReply", 0, FakeModel("test1").avro_bytes),
            HandlerEntry(2, "fooReply", 0, FakeModel("test2").avro_bytes),
            HandlerEntry(3, "fooReply", 0, FakeModel("test3").avro_bytes),
            HandlerEntry(4, "fooReply", 0, FakeModel("test4").avro_bytes),
        ]

        async def _fn():
            await self._insert_one(Message("foo", 0, FakeModel("test1").avro_bytes))
            await self._insert_one(Message("foo", 0, FakeModel("test2").avro_bytes))
            await sleep(0.5)
            await self._insert_one(Message("foo", 0, FakeModel("test3").avro_bytes))
            await self._insert_one(Message("foo", 0, FakeModel("test4").avro_bytes))

        async with self.handler:
            observed, _ = await gather(self.handler.get_many(count=4, max_wait=0.1), _fn())

        self.assertEqual(len(expected), len(observed))
        for e, o in zip(expected, observed):
            self._assert_equal_entries(e, o)

    async def test_get_many_raises(self):
        async with self.handler:
            with self.assertRaises(MinosHandlerNotFoundEnoughEntriesException):
                await self.handler.get_many(count=3, timeout=0.1)

    async def _insert_one(self, instance):
        async with aiopg.connect(**self.broker_queue_db) as connect:
            async with connect.cursor() as cur:
                await cur.execute(
                    "INSERT INTO dynamic_queue (topic, partition_id, binary_data, creation_date) "
                    "VALUES (%s, %s, %s, NOW()) "
                    "RETURNING id;",
                    (f"{instance.topic}Reply", 0, instance.value),
                )
                return (await cur.fetchone())[0]

    def _assert_equal_entries(self, expected, observed):
        self.assertEqual(expected.id, observed.id)
        self.assertEqual(expected.topic, observed.topic)
        self.assertEqual(expected.callback, observed.callback)
        self.assertEqual(expected.partition_id, observed.partition_id)
        self.assertEqual(expected.data, observed.data)
        self.assertEqual(expected.retry, observed.retry)
        self.assertAlmostEqual(expected.created_at, observed.created_at, delta=timedelta(seconds=2))


if __name__ == "__main__":
    unittest.main()
