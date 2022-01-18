# noqa: F821

import asyncio
import unittest
from asyncio import (
    gather,
    sleep,
)
from unittest.mock import (
    AsyncMock,
    call,
)
from uuid import (
    uuid4,
)

import aiopg

from minos.common import (
    NotProvidedException,
)
from minos.common.testing import (
    PostgresAsyncTestCase,
)
from minos.networks import (
    BrokerConsumer,
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
class TestProducer(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def setUp(self) -> None:
        super().setUp()

        self.consumer = BrokerConsumer.from_config(self.config)
        self.producer = PostgreSqlBrokerPublisherRepositoryDequeue.from_config(  # noqa: F821
            self.config, consumer=self.consumer
        )

    async def asyncSetUp(self):
        await super().asyncSetUp()
        await self.consumer.setup()
        await self.producer.setup()

    async def asyncTearDown(self):
        await self.producer.destroy()
        await self.consumer.destroy()
        await super().asyncTearDown()

    def test_from_config_default(self):
        self.assertIsInstance(self.producer, PostgreSqlBrokerPublisherRepositoryDequeue)  # noqa: F821

    async def test_from_config_raises(self):
        with self.assertRaises(NotProvidedException):
            PostgreSqlBrokerPublisherRepositoryDequeue.from_config(config=self.config)  # noqa: F821

    async def test_dispatch_one_internal_true(self):
        mock = AsyncMock()
        self.consumer.enqueue = mock

        ok = await self.producer.dispatch_one((0, "GetOrder", bytes(), BrokerMessageV1Strategy.UNICAST))
        self.assertTrue(ok)

        self.assertEqual(1, mock.call_count)
        self.assertEqual(call("GetOrder", -1, bytes()), mock.call_args)

    async def test_dispatch_one_internal_false(self):
        self.producer.consumer = None

        publish_mock = AsyncMock()
        self.producer.publish = publish_mock

        ok = await self.producer.dispatch_one((0, "GetOrder", bytes(), BrokerMessageV1Strategy.UNICAST))
        self.assertTrue(ok)

        self.assertEqual(1, publish_mock.call_count)
        self.assertEqual(call("GetOrder", bytes()), publish_mock.call_args)

    async def test_dispatch_one_external_true(self):
        mock = AsyncMock()
        self.producer.publish = mock

        ok = await self.producer.dispatch_one((0, "GetProduct", bytes(), BrokerMessageV1Strategy.UNICAST))
        self.assertTrue(ok)

        self.assertEqual(1, mock.call_count)
        self.assertEqual(call("GetProduct", bytes()), mock.call_args)

    async def test_dispatch_one_external_true_event(self):
        mock = AsyncMock()
        self.producer.publish = mock
        ok = await self.producer.dispatch_one((0, "TicketAdded", bytes(), BrokerMessageV1Strategy.MULTICAST))
        self.assertTrue(ok)
        self.assertEqual(1, mock.call_count)
        self.assertEqual(call("TicketAdded", bytes()), mock.call_args)

    async def test_dispatch_one_external_false(self):
        self.producer.publish = AsyncMock(return_value=False)
        ok = await self.producer.dispatch_one((0, "GetOrder", bytes(), BrokerMessageV1Strategy.MULTICAST))
        self.assertFalse(ok)

    async def test_publish_true(self):
        ok = await self.producer.publish(topic="TestKafkaSend", message=bytes())
        self.assertTrue(ok)

    async def test_publish_false(self):
        self.producer.client.send_and_wait = AsyncMock(side_effect=ValueError)
        ok = await self.producer.publish(topic="TestKafkaSend", message=bytes())
        self.assertFalse(ok)

    async def test_dispatch_forever(self):
        mock = AsyncMock(side_effect=ValueError)

        self.producer.dispatch = mock
        try:
            await gather(self.producer.dispatch_forever(), self._notify("producer_queue"))
        except ValueError:
            pass
        self.assertEqual(1, mock.call_count)

    async def test_dispatch_forever_without_notify(self):
        mock_dispatch = AsyncMock(side_effect=[None, ValueError])
        mock_count = AsyncMock(side_effect=[1, 0, 1])

        self.producer.dispatch = mock_dispatch
        self.producer._get_count = mock_count
        try:
            await self.producer.dispatch_forever(max_wait=0.01)
        except ValueError:
            pass

        self.assertEqual(2, mock_dispatch.call_count)
        self.assertEqual(3, mock_count.call_count)

    async def test_concurrency_dispatcher(self):
        model = FakeModel("foo")
        identifier = uuid4()

        broker_publisher = PostgreSqlBrokerPublisherRepositoryEnqueue.from_config(config=self.config)  # noqa: F821

        async with broker_publisher:
            for x in range(60):
                await broker_publisher.send(
                    BrokerMessageV1(
                        "CommandBroker-Delete",
                        BrokerMessageV1Payload(model),
                        identifier=identifier,
                        reply_topic="TestDeleteReply",
                    )
                )

        async with aiopg.connect(**self.broker_queue_db) as connect:
            async with connect.cursor() as cur:
                await cur.execute("SELECT COUNT(*) FROM producer_queue")
                records = await cur.fetchone()

        assert records[0] == 60

        await asyncio.gather(*(self.producer.dispatch() for _ in range(6)))

        async with aiopg.connect(**self.broker_queue_db) as connect:
            async with connect.cursor() as cur:
                await cur.execute("SELECT COUNT(*) FROM producer_queue")
                records = await cur.fetchone()

        assert records[0] == 0

    async def test_if_commands_was_deleted(self):
        async with PostgreSqlBrokerPublisherRepositoryEnqueue.from_config(  # noqa: F821
            config=self.config
        ) as broker_publisher:
            await broker_publisher.send(BrokerMessageV1("TestDeleteReply", BrokerMessageV1Payload(FakeModel("Foo"))))
            await broker_publisher.send(BrokerMessageV1("TestDeleteReply", BrokerMessageV1Payload(FakeModel("Foo"))))

        await self.producer.dispatch()

        async with aiopg.connect(**self.broker_queue_db) as connection:
            async with connection.cursor() as cursor:
                await cursor.execute("SELECT COUNT(*) FROM producer_queue WHERE topic = '%s'" % "TestDeleteReply")
                self.assertEqual(0, (await cursor.fetchone())[0])

    async def test_if_commands_retry_was_incremented(self):
        model = FakeModel("foo")
        identifier = uuid4()

        async with PostgreSqlBrokerPublisherRepositoryEnqueue.from_config(  # noqa: F821
            config=self.config
        ) as broker_publisher:
            await broker_publisher.send(
                BrokerMessageV1(
                    "TestDeleteOrderReply",
                    identifier=identifier,
                    payload=BrokerMessageV1Payload(content=model, status=BrokerMessageV1Status.SUCCESS),
                )
            )
            await broker_publisher.send(
                BrokerMessageV1(
                    "TestDeleteOrderReply",
                    identifier=identifier,
                    payload=BrokerMessageV1Payload(content=model, status=BrokerMessageV1Status.SUCCESS),
                )
            )

            self.producer.publish = AsyncMock(return_value=False)
            await self.producer.dispatch()

        async with aiopg.connect(**self.broker_queue_db) as connection:
            async with connection.cursor() as cursor:
                await cursor.execute("SELECT COUNT(*) FROM producer_queue WHERE topic = 'TestDeleteOrderReply'")
                self.assertEqual(2, (await cursor.fetchone())[0])

                await cursor.execute("SELECT retry FROM producer_queue WHERE id=1;")
                self.assertEqual(1, (await cursor.fetchone())[0])

                await cursor.execute("SELECT retry FROM producer_queue WHERE id=2;")
                self.assertEqual(1, (await cursor.fetchone())[0])

    async def _notify(self, name):
        await sleep(0.2)
        async with aiopg.connect(**self.broker_queue_db) as connect:
            async with connect.cursor() as cur:
                await cur.execute(f"NOTIFY {name!s};")


if __name__ == "__main__":
    unittest.main()
