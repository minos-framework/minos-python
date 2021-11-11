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

from minos.common.testing import (
    PostgresAsyncTestCase,
)
from minos.networks import (
    CommandBroker,
    CommandReplyBroker,
    CommandStatus,
    Consumer,
    EventBroker,
    Producer,
)
from tests.utils import (
    BASE_PATH,
    FAKE_AGGREGATE_DIFF,
    FakeModel,
)


class TestProducer(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def setUp(self) -> None:
        super().setUp()

        self.consumer = Consumer.from_config(config=self.config)
        self.producer = Producer.from_config(config=self.config, consumer=self.consumer)

    def test_from_config_default(self):
        self.assertIsInstance(self.producer, Producer)

    async def test_dispatch_one_internal_true(self):
        mock = AsyncMock()
        self.consumer.enqueue = mock

        async with self.producer:
            ok = await self.producer.dispatch_one((0, "GetOrder", bytes(), "command"))
            self.assertTrue(ok)

        self.assertEqual(1, mock.call_count)
        self.assertEqual(call("GetOrder", -1, bytes()), mock.call_args)

    async def test_dispatch_one_internal_false(self):
        self.producer.consumer = None

        publish_mock = AsyncMock()
        self.producer.publish = publish_mock

        async with self.producer:
            ok = await self.producer.dispatch_one((0, "GetOrder", bytes(), "command"))
            self.assertTrue(ok)

        self.assertEqual(1, publish_mock.call_count)
        self.assertEqual(call("GetOrder", bytes()), publish_mock.call_args)

    async def test_dispatch_one_external_true(self):
        mock = AsyncMock()
        self.producer.publish = mock

        async with self.producer:
            ok = await self.producer.dispatch_one((0, "GetProduct", bytes(), "command"))
            self.assertTrue(ok)

        self.assertEqual(1, mock.call_count)
        self.assertEqual(call("GetProduct", bytes()), mock.call_args)

    async def test_dispatch_one_external_true_event(self):
        mock = AsyncMock()
        self.producer.publish = mock
        async with self.producer:
            ok = await self.producer.dispatch_one((0, "TicketAdded", bytes(), "event"))
            self.assertTrue(ok)
        self.assertEqual(1, mock.call_count)
        self.assertEqual(call("TicketAdded", bytes()), mock.call_args)

    async def test_dispatch_one_external_false(self):
        self.producer.publish = AsyncMock(return_value=False)
        async with self.producer:
            ok = await self.producer.dispatch_one((0, "GetOrder", bytes(), "event"))
            self.assertFalse(ok)

    async def test_publish_true(self):
        async with self.producer:
            ok = await self.producer.publish(topic="TestKafkaSend", message=bytes())
            self.assertTrue(ok)

    async def test_publish_false(self):
        async with self.producer:
            self.producer.client.send_and_wait = AsyncMock(side_effect=ValueError)
            ok = await self.producer.publish(topic="TestKafkaSend", message=bytes())
            self.assertFalse(ok)

    async def test_dispatch_forever(self):
        mock = AsyncMock(side_effect=ValueError)
        async with self.producer:
            self.producer.dispatch = mock
            try:
                await gather(self.producer.dispatch_forever(), self._notify("producer_queue"))
            except ValueError:
                pass
        self.assertEqual(1, mock.call_count)

    async def test_dispatch_forever_without_notify(self):
        mock_dispatch = AsyncMock(side_effect=[None, ValueError])
        mock_count = AsyncMock(side_effect=[1, 0, 1])
        async with self.producer:
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
        saga = uuid4()

        command_broker = CommandBroker.from_config(config=self.config)
        command_reply_broker = CommandReplyBroker.from_config(config=self.config)
        event_broker = EventBroker.from_config(config=self.config)

        for x in range(0, 20):
            async with command_reply_broker:
                await command_reply_broker.send(model, "TestDeleteReply", saga, CommandStatus.SUCCESS)

            async with command_broker:
                await command_broker.send(model, "CommandBroker-Delete", saga, "TestDeleteReply")

            async with event_broker:
                await event_broker.send(FAKE_AGGREGATE_DIFF, topic="EventBroker-Delete")

        async with aiopg.connect(**self.broker_queue_db) as connect:
            async with connect.cursor() as cur:
                await cur.execute("SELECT COUNT(*) FROM producer_queue")
                records = await cur.fetchone()

        assert records[0] == 60

        async with self.producer:
            await asyncio.gather(*(self.producer.dispatch() for _ in range(6)))

        async with aiopg.connect(**self.broker_queue_db) as connect:
            async with connect.cursor() as cur:
                await cur.execute("SELECT COUNT(*) FROM producer_queue")
                records = await cur.fetchone()

        assert records[0] == 0

    async def test_if_commands_was_deleted(self):
        async with EventBroker.from_config(config=self.config) as broker:
            queue_id_1 = await broker.send(FAKE_AGGREGATE_DIFF, "TestDeleteReply")
            queue_id_2 = await broker.send(FAKE_AGGREGATE_DIFF, "TestDeleteReply")

        async with self.producer:
            await self.producer.dispatch()

        async with aiopg.connect(**self.broker_queue_db) as connection:
            async with connection.cursor() as cursor:
                await cursor.execute("SELECT COUNT(*) FROM producer_queue WHERE topic = '%s'" % "TestDeleteReply")
                records = await cursor.fetchone()

        assert queue_id_1 > 0
        assert queue_id_2 > 0
        assert records[0] == 0

    async def test_if_commands_retry_was_incremented(self):
        model = FakeModel("foo")
        saga = uuid4()

        async with CommandReplyBroker.from_config(config=self.config) as broker:
            queue_id_1 = await broker.send(model, "TestDeleteOrderReply", saga, CommandStatus.SUCCESS)
            queue_id_2 = await broker.send(model, "TestDeleteOrderReply", saga, CommandStatus.SUCCESS)

        async with self.producer:
            self.producer.publish = AsyncMock(return_value=False)
            await self.producer.dispatch()

        async with aiopg.connect(**self.broker_queue_db) as connection:
            async with connection.cursor() as cursor:
                await cursor.execute("SELECT COUNT(*) FROM producer_queue WHERE topic = '%s'" % "TestDeleteOrderReply")
                records = await cursor.fetchone()

                await cursor.execute("SELECT retry FROM producer_queue WHERE id=%d;" % queue_id_1)
                retry_1 = await cursor.fetchone()

                await cursor.execute("SELECT retry FROM producer_queue WHERE id=%d;" % queue_id_2)
                retry_2 = await cursor.fetchone()

        assert queue_id_1 > 0
        assert queue_id_2 > 0
        assert records[0] == 2
        assert retry_1[0] > 0
        assert retry_2[0] > 0

    async def _notify(self, name):
        await sleep(0.2)
        async with aiopg.connect(**self.broker_queue_db) as connect:
            async with connect.cursor() as cur:
                await cur.execute(f"NOTIFY {name!s};")


if __name__ == "__main__":
    unittest.main()
