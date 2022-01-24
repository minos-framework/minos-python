import unittest
from asyncio import (
    gather,
    sleep,
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
    BrokerClient,
    BrokerHandlerSetup,
    BrokerMessageV1,
    BrokerMessageV1Payload,
    BrokerPublisher,
    MinosHandlerNotFoundEnoughEntriesException,
)
from tests.utils import (
    BASE_PATH,
    FakeModel,
)


class TestBrokerClient(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def setUp(self) -> None:
        super().setUp()
        self.topic = "fooReply"
        self.publisher = BrokerPublisher.from_config(self.config)
        self.handler = BrokerClient.from_config(config=self.config, topic=self.topic, publisher=self.publisher)

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
            BrokerClient.from_config(config=self.config)

    async def test_setup_destroy(self):
        handler = BrokerClient.from_config(config=self.config, topic=self.topic, publisher=self.publisher)
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

    async def test_receive(self):
        expected = FakeModel("test1")
        await self._insert_one("fooReply", FakeModel("test1").avro_bytes)
        await self._insert_one("fooReply", FakeModel("test2").avro_bytes)

        observed = await self.handler.receive()

        self.assertEqual(expected, observed)

    async def test_receive_many(self):
        expected = [
            FakeModel("test1"),
            FakeModel("test2"),
            FakeModel("test3"),
            FakeModel("test4"),
        ]

        async def _fn1():
            messages = list()
            async for message in self.handler.receive_many(count=4, max_wait=0.1):
                messages.append(message)
            return messages

        async def _fn2():
            await self._insert_one("fooReply", FakeModel("test1").avro_bytes)
            await self._insert_one("fooReply", FakeModel("test2").avro_bytes)
            await sleep(0.5)
            await self._insert_one("fooReply", FakeModel("test3").avro_bytes)
            await self._insert_one("fooReply", FakeModel("test4").avro_bytes)

        observed, _ = await gather(_fn1(), _fn2())

        self.assertEqual(expected, observed)

    async def test_receive_many_raises(self):
        with self.assertRaises(MinosHandlerNotFoundEnoughEntriesException):
            async for _ in self.handler.receive_many(count=3, timeout=0.1):
                pass

    async def _insert_one(self, topic: str, bytes_: bytes):
        async with aiopg.connect(**self.broker_queue_db) as connect:
            async with connect.cursor() as cur:
                await cur.execute(
                    "INSERT INTO consumer_queue (topic, partition, data) VALUES (%s, %s, %s) RETURNING id;",
                    (topic, 0, bytes_),
                )
                return (await cur.fetchone())[0]


if __name__ == "__main__":
    unittest.main()
