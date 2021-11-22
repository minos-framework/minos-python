import unittest
from unittest.mock import (
    AsyncMock,
    call,
)
from uuid import (
    uuid4,
)

from psycopg2.sql import (
    SQL,
)

from minos.common import (
    Model,
)
from minos.common.testing import (
    PostgresAsyncTestCase,
)
from minos.networks import (
    BrokerMessage,
    BrokerMessageStatus,
    BrokerMessageStrategy,
    BrokerPublisher,
)
from tests.utils import (
    BASE_PATH,
    FakeModel,
)


class TestBrokerPublisher(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def setUp(self) -> None:
        super().setUp()
        self.publisher = BrokerPublisher.from_config(self.config)

    async def asyncSetUp(self):
        await super().asyncSetUp()
        await self.publisher.setup()

    async def asyncTearDown(self):
        await self.publisher.destroy()
        await super().asyncTearDown()

    def test_from_config_default(self):
        self.assertIsInstance(BrokerPublisher.from_config(config=self.config), BrokerPublisher)

    async def test_send(self):
        mock = AsyncMock(return_value=56)
        self.publisher.enqueue = mock

        identifier = await self.publisher.send(FakeModel("Foo"), topic="fake")

        self.assertEqual(56, identifier)
        self.assertEqual(1, mock.call_count)

        args = mock.call_args.args
        self.assertEqual("fake", args[0])
        self.assertEqual(BrokerMessageStrategy.UNICAST, args[1])

        expected = BrokerMessage("fake", FakeModel("Foo"), service_name="Order")
        self.assertEqual(expected, Model.from_avro_bytes(args[2]))

    async def test_send_with_reply_topic(self):
        mock = AsyncMock(return_value=56)
        self.publisher.enqueue = mock

        saga = uuid4()

        identifier = await self.publisher.send(FakeModel("foo"), "fake", saga=saga, reply_topic="ekaf")

        self.assertEqual(56, identifier)
        self.assertEqual(1, mock.call_count)

        args = mock.call_args.args
        self.assertEqual("fake", args[0])
        self.assertEqual(BrokerMessageStrategy.UNICAST, args[1])
        expected = BrokerMessage("fake", FakeModel("foo"), saga=saga, reply_topic="ekaf", service_name="Order")
        self.assertEqual(expected, Model.from_avro_bytes(args[2]))

    async def test_send_with_user(self):
        mock = AsyncMock(return_value=56)
        self.publisher.enqueue = mock

        saga = uuid4()
        user = uuid4()

        identifier = await self.publisher.send(FakeModel("foo"), "fake", saga=saga, reply_topic="ekaf", user=user)

        self.assertEqual(56, identifier)
        self.assertEqual(1, mock.call_count)

        args = mock.call_args.args
        self.assertEqual("fake", args[0])
        self.assertEqual(BrokerMessageStrategy.UNICAST, args[1])
        expected = BrokerMessage(
            "fake", FakeModel("foo"), saga=saga, reply_topic="ekaf", user=user, service_name="Order"
        )
        self.assertEqual(expected, Model.from_avro_bytes(args[2]))

    async def test_send_with_status(self):
        mock = AsyncMock(return_value=56)
        self.publisher.enqueue = mock

        saga = uuid4()
        reply_topic = "fakeReply"

        identifier = await self.publisher.send(
            FakeModel("foo"), saga=saga, topic=reply_topic, status=BrokerMessageStatus.SUCCESS
        )

        self.assertEqual(56, identifier)
        self.assertEqual(1, mock.call_count)

        args = mock.call_args.args
        self.assertEqual(reply_topic, args[0])
        self.assertEqual(BrokerMessageStrategy.UNICAST, args[1])

        expected = BrokerMessage(
            reply_topic,
            FakeModel("foo"),
            saga=saga,
            status=BrokerMessageStatus.SUCCESS,
            service_name=self.config.service.name,
        )
        observed = Model.from_avro_bytes(args[2])
        self.assertEqual(expected, observed)

    async def test_send_with_multicast_strategy(self):
        mock = AsyncMock(return_value=56)
        self.publisher.enqueue = mock

        topic = "fakeReply"

        identifier = await self.publisher.send(FakeModel("foo"), topic=topic, strategy=BrokerMessageStrategy.MULTICAST)

        self.assertEqual(56, identifier)
        self.assertEqual(1, mock.call_count)

        args = mock.call_args.args
        self.assertEqual(topic, args[0])
        self.assertEqual(BrokerMessageStrategy.MULTICAST, args[1])

        expected = BrokerMessage(
            topic, FakeModel("foo"), service_name=self.config.service.name, strategy=BrokerMessageStrategy.MULTICAST,
        )
        observed = Model.from_avro_bytes(args[2])
        self.assertEqual(expected, observed)

    async def test_enqueue(self):
        query = SQL("INSERT INTO producer_queue (topic, data, action) VALUES (%s, %s, %s) RETURNING id")

        mock = AsyncMock(return_value=(56,))
        self.publisher.submit_query_and_fetchone = mock

        identifier = await self.publisher.enqueue("test_topic", BrokerMessageStrategy.UNICAST, b"test")

        self.assertEqual(56, identifier)
        self.assertEqual(1, mock.call_count)

        self.assertEqual(call(query, ("test_topic", b"test", BrokerMessageStrategy.UNICAST)), mock.call_args)


if __name__ == "__main__":
    unittest.main()
