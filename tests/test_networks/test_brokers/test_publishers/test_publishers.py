import unittest
from unittest.mock import (
    AsyncMock,
    call,
)
from uuid import (
    UUID,
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
        mock = AsyncMock()
        self.publisher.enqueue = mock

        observed = await self.publisher.send(FakeModel("Foo"), topic="fake")

        self.assertIsInstance(observed, UUID)
        self.assertEqual(1, mock.call_count)

        args = mock.call_args.args
        self.assertEqual("fake", args[0])
        self.assertEqual(BrokerMessageStrategy.UNICAST, args[1])

        expected = BrokerMessage("fake", FakeModel("Foo"), service_name="Order", identifier=observed)
        self.assertEqual(expected, Model.from_avro_bytes(args[2]))

    async def test_send_with_identifier(self):
        mock = AsyncMock()
        self.publisher.enqueue = mock

        identifier = uuid4()
        observed = await self.publisher.send(FakeModel("Foo"), topic="fake", identifier=identifier)

        self.assertEqual(identifier, observed)
        self.assertEqual(1, mock.call_count)

        args = mock.call_args.args
        self.assertEqual("fake", args[0])
        self.assertEqual(BrokerMessageStrategy.UNICAST, args[1])

        expected = BrokerMessage("fake", FakeModel("Foo"), service_name="Order", identifier=identifier)
        self.assertEqual(expected, Model.from_avro_bytes(args[2]))

    async def test_send_with_reply_topic(self):
        mock = AsyncMock()
        self.publisher.enqueue = mock

        observed = await self.publisher.send(FakeModel("foo"), "fake", reply_topic="ekaf")

        self.assertIsInstance(observed, UUID)
        self.assertEqual(1, mock.call_count)

        args = mock.call_args.args
        self.assertEqual("fake", args[0])
        self.assertEqual(BrokerMessageStrategy.UNICAST, args[1])
        expected = BrokerMessage(
            "fake", FakeModel("foo"), identifier=observed, reply_topic="ekaf", service_name="Order"
        )
        self.assertEqual(expected, Model.from_avro_bytes(args[2]))

    async def test_send_with_user(self):
        mock = AsyncMock()
        self.publisher.enqueue = mock

        user = uuid4()

        observed = await self.publisher.send(FakeModel("foo"), "fake", reply_topic="ekaf", user=user)

        self.assertIsInstance(observed, UUID)
        self.assertEqual(1, mock.call_count)

        args = mock.call_args.args
        self.assertEqual("fake", args[0])
        self.assertEqual(BrokerMessageStrategy.UNICAST, args[1])
        expected = BrokerMessage(
            "fake", FakeModel("foo"), identifier=observed, reply_topic="ekaf", user=user, service_name="Order"
        )
        self.assertEqual(expected, Model.from_avro_bytes(args[2]))

    async def test_send_with_status(self):
        mock = AsyncMock()
        self.publisher.enqueue = mock

        reply_topic = "fakeReply"

        observed = await self.publisher.send(FakeModel("foo"), topic=reply_topic, status=BrokerMessageStatus.SUCCESS)

        self.assertIsInstance(observed, UUID)
        self.assertEqual(1, mock.call_count)

        args = mock.call_args.args
        self.assertEqual(reply_topic, args[0])
        self.assertEqual(BrokerMessageStrategy.UNICAST, args[1])

        expected = BrokerMessage(
            reply_topic,
            FakeModel("foo"),
            identifier=observed,
            status=BrokerMessageStatus.SUCCESS,
            service_name=self.config.service.name,
        )
        observed = Model.from_avro_bytes(args[2])
        self.assertEqual(expected, observed)

    async def test_send_with_multicast_strategy(self):
        mock = AsyncMock()
        self.publisher.enqueue = mock

        topic = "fakeReply"

        observed = await self.publisher.send(FakeModel("foo"), topic=topic, strategy=BrokerMessageStrategy.MULTICAST)

        self.assertIsInstance(observed, UUID)
        self.assertEqual(1, mock.call_count)

        args = mock.call_args.args
        self.assertEqual(topic, args[0])
        self.assertEqual(BrokerMessageStrategy.MULTICAST, args[1])

        expected = BrokerMessage(
            topic,
            FakeModel("foo"),
            identifier=observed,
            service_name=self.config.service.name,
            strategy=BrokerMessageStrategy.MULTICAST,
        )
        observed = Model.from_avro_bytes(args[2])
        self.assertEqual(expected, observed)

    async def test_enqueue(self):
        query = SQL("INSERT INTO producer_queue (topic, data, strategy) VALUES (%s, %s, %s) RETURNING id")

        mock = AsyncMock(return_value=(56,))
        self.publisher.submit_query_and_fetchone = mock

        observed = await self.publisher.enqueue("test_topic", BrokerMessageStrategy.UNICAST, b"test")

        self.assertEqual(56, observed)
        self.assertEqual(1, mock.call_count)

        self.assertEqual(call(query, ("test_topic", b"test", BrokerMessageStrategy.UNICAST)), mock.call_args)


if __name__ == "__main__":
    unittest.main()
