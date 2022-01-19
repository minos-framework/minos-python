import unittest
from asyncio import (
    gather,
    sleep,
)
from datetime import (
    timedelta,
)
from unittest.mock import (
    AsyncMock,
    call,
)

import aiopg

from minos.common import (
    NotProvidedException,
)
from minos.common.testing import (
    PostgresAsyncTestCase,
)
from minos.networks import (
    BrokerHandlerEntry,
    BrokerHandlerSetup,
    BrokerMessageV1,
    BrokerMessageV1Payload,
    DynamicBroker,
    InMemoryBrokerPublisher,
    MinosHandlerNotFoundEnoughEntriesException,
)
from tests.utils import (
    BASE_PATH,
    FakeModel,
)


@unittest.skip("FIXME!")
class TestDynamicBroker(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def setUp(self) -> None:
        super().setUp()
        self.topic = "fooReply"
        self.publisher = InMemoryBrokerPublisher.from_config(self.config)
        self.handler = DynamicBroker.from_config(config=self.config, topic=self.topic, publisher=self.publisher)

    async def asyncSetUp(self):
        await super().asyncSetUp()
        await self.publisher.setup()
        await self.handler.setup()

    async def asyncTearDown(self):
        await self.handler.destroy()
        await self.publisher.destroy()
        await super().asyncTearDown()

    async def test_from_config_raises(self):
        with self.assertRaises(NotProvidedException):
            DynamicBroker.from_config(config=self.config)

    async def test_setup_destroy(self):
        handler = DynamicBroker.from_config(config=self.config, topic=self.topic, publisher=self.publisher)
        self.assertFalse(handler.already_setup)
        async with handler:
            self.assertTrue(handler.already_setup)
        self.assertTrue(handler.already_destroyed)

    def test_base_classes(self):
        self.assertIsInstance(self.handler, BrokerHandlerSetup)

    async def test_send(self):
        mock = AsyncMock()
        self.publisher.send = mock
        message = BrokerMessageV1("AddFoo", BrokerMessageV1Payload(56))
        await self.handler.send(message)

        expected = BrokerMessageV1(
            "AddFoo", BrokerMessageV1Payload(56), reply_topic=self.topic, identifier=message.identifier
        )
        self.assertEqual([call(expected)], mock.call_args_list)

    async def test_get_one(self):
        expected = BrokerHandlerEntry(1, "fooReply", 0, FakeModel("test1").avro_bytes)
        await self._insert_one("fooReply", FakeModel("test1").avro_bytes)
        await self._insert_one("fooReply", FakeModel("test2").avro_bytes)

        observed = await self.handler.get_one()

        self._assert_equal_entries(expected, observed)

    async def test_get_many(self):
        expected = [
            BrokerHandlerEntry(1, "fooReply", 0, FakeModel("test1").avro_bytes),
            BrokerHandlerEntry(2, "fooReply", 0, FakeModel("test2").avro_bytes),
            BrokerHandlerEntry(3, "fooReply", 0, FakeModel("test3").avro_bytes),
            BrokerHandlerEntry(4, "fooReply", 0, FakeModel("test4").avro_bytes),
        ]

        async def _fn():
            await self._insert_one("fooReply", FakeModel("test1").avro_bytes)
            await self._insert_one("fooReply", FakeModel("test2").avro_bytes)
            await sleep(0.5)
            await self._insert_one("fooReply", FakeModel("test3").avro_bytes)
            await self._insert_one("fooReply", FakeModel("test4").avro_bytes)

        observed, _ = await gather(self.handler.get_many(count=4, max_wait=0.1), _fn())

        self.assertEqual(len(expected), len(observed))
        for e, o in zip(expected, observed):
            self._assert_equal_entries(e, o)

    async def test_get_many_raises(self):
        with self.assertRaises(MinosHandlerNotFoundEnoughEntriesException):
            await self.handler.get_many(count=3, timeout=0.1)

    async def _insert_one(self, topic: str, bytes_: bytes):
        async with aiopg.connect(**self.broker_queue_db) as connect:
            async with connect.cursor() as cur:
                await cur.execute(
                    "INSERT INTO consumer_queue (topic, partition, data) VALUES (%s, %s, %s) RETURNING id;",
                    (topic, 0, bytes_),
                )
                return (await cur.fetchone())[0]

    def _assert_equal_entries(self, expected: BrokerHandlerEntry, observed: BrokerHandlerEntry):
        self.assertEqual(expected.id, observed.id)
        self.assertEqual(expected.topic, observed.topic)
        self.assertEqual(expected.partition, observed.partition)
        self.assertEqual(expected.data, observed.data)
        self.assertEqual(expected.retry, observed.retry)
        self.assertAlmostEqual(expected.created_at, observed.created_at, delta=timedelta(seconds=2))


if __name__ == "__main__":
    unittest.main()
