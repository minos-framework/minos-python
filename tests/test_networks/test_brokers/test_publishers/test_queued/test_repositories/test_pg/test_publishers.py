# noqa: F821

import unittest
from unittest.mock import (
    AsyncMock,
    call,
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

        message = BrokerMessageV1("fake", BrokerMessageV1Payload(FakeModel("Foo")))
        await self.publisher.send(message)

        self.assertEqual(1, mock.call_count)

        args = mock.call_args.args

        self.assertEqual("fake", args[0])
        self.assertEqual(BrokerMessageV1Strategy.UNICAST, args[1])
        self.assertEqual(message, Model.from_avro_bytes(args[2]))

    async def test_send_with_multicast_strategy(self):
        mock = AsyncMock()
        self.publisher.enqueue = mock

        message = BrokerMessageV1(
            "fake", BrokerMessageV1Payload(FakeModel("foo")), strategy=BrokerMessageV1Strategy.MULTICAST
        )
        await self.publisher.send(message)

        self.assertEqual(1, mock.call_count)

        args = mock.call_args.args
        self.assertEqual("fake", args[0])
        self.assertEqual(BrokerMessageV1Strategy.MULTICAST, args[1])
        self.assertEqual(message, Model.from_avro_bytes(args[2]))

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
