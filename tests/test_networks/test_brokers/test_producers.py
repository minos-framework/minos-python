import asyncio
import unittest

import aiopg

from minos.common import (
    CommandStatus,
    MinosConfig,
)
from minos.common.testing import (
    PostgresAsyncTestCase,
)
from minos.networks import (
    CommandBroker,
    CommandReplyBroker,
    EventBroker,
    Producer,
)
from tests.utils import (
    BASE_PATH,
    FakeModel,
)


class TestProducer(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def test_from_config_default(self):
        self.assertIsInstance(Producer.from_config(config=self.config), Producer)

    async def test_send_to_kafka_ok(self):
        dispatcher = Producer.from_config(config=self.config)
        response = await dispatcher.publish(topic="TestKafkaSend", message=bytes())
        assert response is True

    async def test_concurrency_dispatcher(self):
        model = FakeModel("foo")

        command_broker = CommandBroker.from_config(
            "CommandBroker-Delete", config=self.config, saga_uuid="9347839473kfslf", reply_topic="TestDeleteReply"
        )
        command_reply_broker = CommandReplyBroker.from_config(
            "TestDeleteReply", config=self.config, saga_uuid="9347839473kfslf", status=CommandStatus.SUCCESS
        )
        event_broker = EventBroker.from_config("EventBroker-Delete", config=self.config)

        for x in range(0, 20):
            async with command_reply_broker:
                await command_reply_broker.send_one(model)

            async with command_broker:
                await command_broker.send_one(model)

            async with event_broker:
                await event_broker.send_one(model)

        async with aiopg.connect(**self.events_queue_db) as connect:
            async with connect.cursor() as cur:
                await cur.execute("SELECT COUNT(*) FROM producer_queue")
                records = await cur.fetchone()

        assert records[0] == 60

        handler = Producer.from_config(config=self.config)

        await asyncio.gather(*[handler.dispatch() for i in range(0, 6)])

        async with aiopg.connect(**self.events_queue_db) as connect:
            async with connect.cursor() as cur:
                await cur.execute("SELECT COUNT(*) FROM producer_queue")
                records = await cur.fetchone()

        assert records[0] == 0

    async def test_if_commands_was_deleted(self):
        model = FakeModel("foo")

        async with EventBroker.from_config("TestDeleteReply", config=self.config) as broker:
            queue_id_1 = await broker.send_one(model)
            queue_id_2 = await broker.send_one(model)

        await Producer.from_config(config=self.config).dispatch()

        async with aiopg.connect(**self.events_queue_db) as connection:
            async with connection.cursor() as cursor:
                await cursor.execute("SELECT COUNT(*) FROM producer_queue WHERE topic = '%s'" % "TestDeleteReply")
                records = await cursor.fetchone()

        assert queue_id_1 > 0
        assert queue_id_2 > 0
        assert records[0] == 0

    async def test_if_commands_retry_was_incremented(self):
        model = FakeModel("foo")

        async with CommandReplyBroker.from_config(
            "TestDeleteOrder", config=self.config, saga_uuid="9347839473kfslf", status=CommandStatus.SUCCESS
        ) as broker:
            queue_id_1 = await broker.send_one(model)
            queue_id_2 = await broker.send_one(model)

        config = MinosConfig(
            path=BASE_PATH / "wrong_test_config.yml",
            events_queue_database=self.config.events.queue.database,
            events_queue_user=self.config.events.queue.user,
        )
        await Producer.from_config(config=config).dispatch()

        async with aiopg.connect(**self.events_queue_db) as connection:
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


if __name__ == "__main__":
    unittest.main()
