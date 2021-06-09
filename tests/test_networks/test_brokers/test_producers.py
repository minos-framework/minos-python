import asyncio
import unittest

import aiopg

from minos.common import (
    MinosConfigException,
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

    def test_from_config(self):
        dispatcher = Producer.from_config(config=self.config)
        self.assertIsInstance(dispatcher, Producer)

    def test_from_config_default(self):
        self.assertIsInstance(Producer.from_config(config=self.config), Producer)

    def test_from_config_raises(self):
        with self.assertRaises(MinosConfigException):
            Producer.from_config()

    async def test_send_to_kafka_ok(self):
        dispatcher = Producer.from_config(config=self.config)
        response = await dispatcher.publish(topic="TestKafkaSend", message=bytes())
        assert response is True

    async def test_concurrency_dispatcher(self):
        model = FakeModel("foo")

        for x in range(0, 20):
            async with CommandReplyBroker.from_config(
                "TestDeleteReply", config=self.config, saga_uuid="9347839473kfslf"
            ) as broker:
                await broker.send_one(model)

            async with CommandBroker.from_config(
                "CommandBroker-Delete", config=self.config, saga_uuid="9347839473kfslf", reply_on="test_reply_on",
            ) as broker:
                await broker.send_one(model)

            async with EventBroker.from_config("EventBroker-Delete", config=self.config) as broker:
                await broker.send_one(model)

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


if __name__ == "__main__":
    unittest.main()
