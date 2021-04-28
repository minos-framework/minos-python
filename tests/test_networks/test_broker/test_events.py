import unittest

import aiopg
from minos.common import (
    Aggregate,
    MinosConfig,
)
from minos.common.testing import (
    PostgresAsyncTestCase,
)

from minos.networks import (
    MinosEventBroker,
    MinosQueueDispatcher,
)
from tests.utils import (
    BASE_PATH,
)


class AggregateTest(Aggregate):
    test: int


class TestMinosEventBroker(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yaml"

    async def test_if_queue_table_exists(self):
        broker = MinosEventBroker.from_config("EventBroker", config=self.config)
        await broker.setup()

        async with aiopg.connect(**self.events_queue_db) as connection:
            async with connection.cursor() as cursor:
                await cursor.execute(
                    "SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'producer_queue';"
                )
                ret = []
                async for row in cursor:
                    ret.append(row)

        assert ret == [(1,)]

    async def test_send_to_kafka_ok(self):
        dispatcher = MinosQueueDispatcher.from_config(config=self.config)
        response = await dispatcher.send_to_kafka(topic="TestKafkaSend", message=bytes())
        assert response is True

    async def test_events_broker_insertion(self):
        broker = MinosEventBroker.from_config("EventBroker", config=self.config)
        await broker.setup()

        a = AggregateTest(test_id=1, test=2, id=1, version=1)
        affected_rows, queue_id = await broker.send_one(a)

        assert affected_rows == 1
        assert queue_id > 0

    async def test_if_events_was_deleted(self):
        broker = MinosEventBroker.from_config("EventBroker-Delete", config=self.config)
        await broker.setup()

        a = AggregateTest(test_id=1, test=2, id=1, version=1)
        affected_rows_1, queue_id_1 = await broker.send_one(a)
        affected_rows_2, queue_id_2 = await broker.send_one(a)

        await MinosQueueDispatcher.from_config(config=self.config).dispatch()

        async with aiopg.connect(**self.events_queue_db) as connection:
            async with connection.cursor() as cursor:
                await cursor.execute("SELECT COUNT(*) FROM producer_queue WHERE topic = '%s'" % "EventBroker-Delete")
                records = await cursor.fetchone()

        assert affected_rows_1 == 1
        assert queue_id_1 > 0
        assert affected_rows_2 == 1
        assert queue_id_2 > 0
        assert records[0] == 0

    async def test_if_events_retry_was_incremented(self):
        broker = MinosEventBroker.from_config("EventBroker-Delete", config=self.config)
        await broker.setup()

        a = AggregateTest(test_id=1, test=2, id=1, version=1)

        affected_rows_1, queue_id_1 = await broker.send_one(a)
        affected_rows_2, queue_id_2 = await broker.send_one(a)

        config = MinosConfig(
            path=BASE_PATH / "wrong_test_config.yaml", events_queue_database="test_db", events_queue_user="test_user"
        )

        await MinosQueueDispatcher.from_config(config=config).dispatch()

        async with aiopg.connect(**self.events_queue_db) as connection:
            async with connection.cursor() as cursor:
                await cursor.execute("SELECT COUNT(*) FROM producer_queue WHERE topic = '%s'" % "EventBroker-Delete")
                records = await cursor.fetchone()

                await cursor.execute("SELECT retry FROM producer_queue WHERE id=%d;" % queue_id_1)
                retry_1 = await cursor.fetchone()

                await cursor.execute("SELECT retry FROM producer_queue WHERE id=%d;" % queue_id_2)
                retry_2 = await cursor.fetchone()

        assert affected_rows_1 == 1
        assert queue_id_1 > 0
        assert affected_rows_2 == 1
        assert queue_id_2 > 0
        assert records[0] == 2
        assert retry_1[0] > 0
        assert retry_2[0] > 0


if __name__ == "__main__":
    unittest.main()
