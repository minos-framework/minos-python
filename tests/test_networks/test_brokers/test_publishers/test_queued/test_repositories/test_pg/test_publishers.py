# noqa: F821

import unittest
import warnings
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
    BrokerMessageV1,
    BrokerMessageV1Payload,
    BrokerMessageV1Status,
    BrokerMessageV1Strategy,
)
from tests.utils import (
    BASE_PATH,
    FakeModel,
)


@unittest.skip("FIXME")
class TestBrokerPublisher(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def setUp(self) -> None:
        super().setUp()
        self.publisher = PostgreSqlBrokerPublisherRepositoryEnqueue.from_config(self.config)  # noqa: F821

    async def asyncSetUp(self):
        await super().asyncSetUp()
        await self.publisher.setup()

    async def asyncTearDown(self):
        await self.publisher.destroy()
        await super().asyncTearDown()

    def test_from_config_default(self):
        self.assertIsInstance(
            PostgreSqlBrokerPublisherRepositoryEnqueue.from_config(config=self.config),  # noqa: F821
            PostgreSqlBrokerPublisherRepositoryEnqueue,  # noqa: F821
        )

    async def test_send(self):
        mock = AsyncMock()
        self.publisher.enqueue = mock

        observed = await self.publisher.send(FakeModel("Foo"), "fake")

        self.assertIsInstance(observed, UUID)
        self.assertEqual(1, mock.call_count)

        args = mock.call_args.args
        self.assertEqual("fake", args[0])
        self.assertEqual(BrokerMessageV1Strategy.UNICAST, args[1])

        expected = BrokerMessageV1("fake", BrokerMessageV1Payload(FakeModel("Foo")), identifier=observed)
        self.assertEqual(expected, Model.from_avro_bytes(args[2]))

    async def test_send_with_identifier(self):
        mock = AsyncMock()
        self.publisher.enqueue = mock

        identifier = uuid4()
        observed = await self.publisher.send(FakeModel("Foo"), "fake", identifier=identifier)

        self.assertEqual(identifier, observed)
        self.assertEqual(1, mock.call_count)

        args = mock.call_args.args
        self.assertEqual("fake", args[0])
        self.assertEqual(BrokerMessageV1Strategy.UNICAST, args[1])

        expected = BrokerMessageV1(
            topic="fake", payload=BrokerMessageV1Payload(content=FakeModel("Foo")), identifier=identifier
        )
        self.assertEqual(expected, Model.from_avro_bytes(args[2]))

    async def test_send_with_reply_topic(self):
        mock = AsyncMock()
        self.publisher.enqueue = mock

        observed = await self.publisher.send(FakeModel("foo"), "fake", reply_topic="ekaf")

        self.assertIsInstance(observed, UUID)
        self.assertEqual(1, mock.call_count)

        args = mock.call_args.args
        self.assertEqual("fake", args[0])
        self.assertEqual(BrokerMessageV1Strategy.UNICAST, args[1])
        expected = BrokerMessageV1(
            topic="fake",
            payload=BrokerMessageV1Payload(content=FakeModel("foo")),
            identifier=observed,
            reply_topic="ekaf",
        )
        self.assertEqual(expected, Model.from_avro_bytes(args[2]))

    async def test_send_with_user(self):
        mock = AsyncMock()
        self.publisher.enqueue = mock

        user = uuid4()

        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            observed = await self.publisher.send(FakeModel("foo"), "fake", reply_topic="ekaf", user=user)

        self.assertIsInstance(observed, UUID)
        self.assertEqual(1, mock.call_count)

        args = mock.call_args.args
        self.assertEqual("fake", args[0])
        self.assertEqual(BrokerMessageV1Strategy.UNICAST, args[1])
        expected = BrokerMessageV1(
            topic="fake",
            payload=BrokerMessageV1Payload(content=FakeModel("foo"), headers={"User": str(user)}),
            identifier=observed,
            reply_topic="ekaf",
        )
        self.assertEqual(expected, Model.from_avro_bytes(args[2]))

    async def test_send_with_status(self):
        mock = AsyncMock()
        self.publisher.enqueue = mock

        observed = await self.publisher.send(FakeModel("foo"), "fake", status=BrokerMessageV1Status.ERROR)

        self.assertIsInstance(observed, UUID)
        self.assertEqual(1, mock.call_count)

        args = mock.call_args.args
        self.assertEqual("fake", args[0])
        self.assertEqual(BrokerMessageV1Strategy.UNICAST, args[1])

        expected = BrokerMessageV1(
            topic="fake",
            payload=BrokerMessageV1Payload(content=FakeModel("foo"), status=BrokerMessageV1Status.ERROR),
            identifier=observed,
        )
        observed = Model.from_avro_bytes(args[2])
        self.assertEqual(expected, observed)

    async def test_send_with_multicast_strategy(self):
        mock = AsyncMock()
        self.publisher.enqueue = mock

        observed = await self.publisher.send(FakeModel("foo"), "fake", strategy=BrokerMessageV1Strategy.MULTICAST)

        self.assertIsInstance(observed, UUID)
        self.assertEqual(1, mock.call_count)

        args = mock.call_args.args
        self.assertEqual("fake", args[0])
        self.assertEqual(BrokerMessageV1Strategy.MULTICAST, args[1])

        expected = BrokerMessageV1(
            topic="fake",
            payload=BrokerMessageV1Payload(content=FakeModel("foo")),
            identifier=observed,
            strategy=BrokerMessageV1Strategy.MULTICAST,
        )
        observed = Model.from_avro_bytes(args[2])
        self.assertEqual(expected, observed)

    async def test_enqueue(self):
        query = SQL("INSERT INTO producer_queue (topic, data, strategy) VALUES (%s, %s, %s) RETURNING id")

        mock = AsyncMock(return_value=(56,))
        self.publisher.submit_query_and_fetchone = mock

        observed = await self.publisher.enqueue("test_topic", BrokerMessageV1Strategy.UNICAST, b"test")

        self.assertEqual(56, observed)
        self.assertEqual(1, mock.call_count)

        self.assertEqual(call(query, ("test_topic", b"test", BrokerMessageV1Strategy.UNICAST)), mock.call_args)


if __name__ == "__main__":
    unittest.main()
